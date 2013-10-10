from datetime import datetime, timedelta
import os
import subprocess
import urllib
from dzAlerts.util import struct
from dzAlerts.util.basic import nvl
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import DB
from dzAlerts.util.elasticsearch import ElasticSearch
from dzAlerts.util.files import File
from dzAlerts.util.logs import Log
from dzAlerts.util.query import Q
from dzAlerts.util.startup import startup
from dzAlerts.util.strings import between
from dzAlerts.util.struct import Null
from dzAlerts.util.timer import Timer

DEBUG = True

TEMPLATE = """
changeset = "{date|hgdate|urlescape}\\t{node}\\t{rev}\\t{author|urlescape}\\t{branches}\\t{files}\\t{file_adds}\\t{file_dels}\\t{parents}\\t{tags}\\t{desc|urlescape}\\n"
branch = "{branch}%0A"
file = "{file}%0A"
file_add = "{file_add}%0A"
file_del = "{file_del}%0A"
parent = "{parent}%0A"
tag = "{tag}%0A"
"""
TEMPLATE_FILE = File("C:/Users/klahnakoski/git/datazilla-alerts/tests/resources/hg/changeset.template")

def pull_repo(repo):
    if not File(os.path.join(repo.directory, ".hg")).exists:
        File(repo.directory).delete()

        #REPO DOES NOT EXIST, CLONE IT
        with Timer("Clone hg log for {{name}}", {"name":repo.name}):
            proc = subprocess.Popen(
                ["hg", "clone", repo.url, File(repo.directory).filename],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1
            )
            try:
                while True:
                    line = proc.stdout.readline()
                    if line.startswith("abort:"):
                        Log.error("Can not clone {{repo.url}}, beacuse {{problem}}", {
                            "repo": repo,
                            "problem": line
                        })
                    if line == '':
                        break
                    Log.note("Mercurial cloning: {{status}}", {"status": line})
            finally:
                proc.wait()

    else:
        hgrc_file = File(os.path.join(repo.directory, ".hg", "hgrc"))
        if not hgrc_file.exists:
            hgrc_file.write("[paths]\ndefault = " + repo.url + "\n")

        #REPO EXISTS, PULL TO UPDATE
        with Timer("Pull hg log for {{name}}", {"name":repo.name}):
            proc = subprocess.Popen(
                ["hg", "pull", "--cwd", File(repo.directory).filename],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1
            )
            (output, _) = proc.communicate()

            if output.find("abort: repository default not found!") >= 0:
                File(repo.directory).delete()
                pull_repo(repo)
                return
            if output.find("abort: abandoned transaction found") >= 0:
                File(repo.directory).delete()
                pull_repo(repo)
                return
            if output.find("abort: ") >= 0:
                Log.error("Problem with pull {{reason}}", {"reason": between(output, "abort:", "\n")})

            Log.note("Mercurial pull results:\n{{pull_results}}", {"pull_results": output})



def get_changesets(date_range, repo):
    #MAKE TEMPLATE FILE
    TEMPLATE_FILE.write(TEMPLATE)

    if date_range.max == Null:
        if date_range.min == Null:
            drange = ">0 0"
        else:
            drange = ">" + unicode(CNV.datetime2unix(date_range.min)) + " 0"
    else:
        if date_range.min == Null:
            drange = "<" + unicode(CNV.datetime2unix(date_range.max) - 1) + " 0"
        else:
            drange = unicode(CNV.datetime2unix(date_range.min)) + " 0 to " + unicode(
                CNV.datetime2unix(date_range.max) - 1) + " 0"


    #GET ALL CHANGESET INFO
    args = [
        "hg",
        "log",
        "--cwd",
        File(repo.directory).filename,
        "-v",
        "--date",
        drange,
        "--style",
        TEMPLATE_FILE.filename
    ]

    proc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1
    )

    def iterator():
        try:
            while True:
                try:
                    line = proc.stdout.readline()
                    if line == '':
                        proc.wait()
                        if proc.returncode:
                            Log.error("Unable to pull hg log: return code {{return_code}}", {
                                "return_code": proc.returncode
                            })
                        return
                except Exception, e:
                    Log.error("Problem getting another line", e)

                if line.strip() == "":
                    continue
                Log.note(line)

                (
                    date,
                    node,
                    rev,
                    author,
                    branches,
                    files,
                    file_adds,
                    file_dels,
                    parents,
                    tags,
                    desc
                ) = (CNV.latin12unicode(urllib.unquote(c)) for c in line.split("\t"))

                file_adds = set(file_adds.split("\n")) - {""}
                file_dels = set(file_dels.split("\n")) - {""}
                files = set(files.split("\n")) - set()
                doc = {
                    "repo": repo.name,
                    "date": CNV.unix2datetime(CNV.value2number(date.split(" ")[0])),
                    "node": node,
                    "revision": rev,
                    "author": author,
                    "branches": set(branches.split("\n")) - {""},
                    "file_changes": files - file_adds - file_dels,
                    "file_adds": file_adds,
                    "file_dels": file_dels,
                    "parents": set(parents.split("\n")) - {""},
                    "tags": set(tags.split("\n")) - {""},
                    "description": desc
                }
                doc = ElasticSearch.scrub(doc)
                yield doc
        except Exception, e:
            if isinstance(e, ValueError) and e.message.startswith("need more than "):
                Log.error("Problem iterating through log ({{message}})", {
                    "message": line
                }, e)


            Log.error("Problem iterating through log", e)

    return iterator()


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        with DB(settings.database) as db:
            for repo in settings.param.repos:
                try:
                    pull_repo(repo)

                    #GET LATEST DATE
                    existing_range = db.query("""
                        SELECT
                            max(`date`) `max`,
                            min(`date`) `min`
                        FROM
                            changesets
                        WHERE
                            repo={{repo}}
                    """, {"repo": repo.name})[0]

                    ranges = struct.wrap([
                        {"min": nvl(existing_range.max, CNV.milli2datetime(0)) + timedelta(0, 1)},
                        {"max": existing_range.min}
                    ])

                    for r in ranges:
                        for g, docs in Q.groupby(get_changesets(r, repo), size=100):
                            for doc in docs:
                                doc.file_changes = Null
                                doc.file_adds = Null
                                doc.file_dels = Null
                                doc.description = doc.description[0:16000]
                                db.insert("changesets", doc)
                            db.flush()
                except Exception, e:
                    Log.warning("Failure to pull from {{repo.name}}", {"repo":repo}, e)
    finally:
        Log.stop()


main()


# hg log -v -l 20 --template "{date}\t{node}\t{rev}\t{author|urlescape}\t{branches}\t{files}\t{file_adds}\t{file_dels}\t{parents}\t{tags}\t{desc|urlescape}\n"
#
#
#
#
# hg log -v -l 20 --style "C:\Users\klahnakoski\git\datazilla-alerts\tests\resources\hg\changeset.template"


