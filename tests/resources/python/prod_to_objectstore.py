
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
import functools

from math import floor
import requests
from util.basic import nvl
from util.debug import D
from util.query import Q
from util.startup import startup
from util.timer import Timer
from util.db import DB, SQL
from util.cnv import CNV
from util.multithread import Multithread
from util.threads import Lock


db_lock=Lock("db lock")


def etl(name, db, settings, id):
    try:
        with Timer(str(name)+" read from prod "+str(id)):
            content = requests.get(settings.source.service.blob_url + "/" + str(id), timeout=30).content
            if content.startswith("Id not found:"):
                D.println("Id not found: {{id}}", {"id":id})
                return False
            data=CNV.JSON2object(content)

        with Timer(str(name)+" push to local "+str(id)):
            with db_lock:
                db.insert("objectstore", {
                    "id":id,
                    "test_run_id":data.test_run_id,
                    "date_loaded":data.date_loaded,
                    "revision":data.json_blob.test_build.revision,
                    "branch":data.json_blob.test_build.branch,
                    "json_blob":CNV.object2JSON(data.json_blob),
                })
                db.flush()
                return True
    except Exception, e:
        D.warning("Can not load {{id}}", {"id":id}, e)
        return False




def get_existing_ids(db):
    #FIND WHAT'S MISSING IN LOCAL ALREADY
    ranges = db.query("""
	    SELECT DISTINCT
			id,
			`end`
		FROM (
			SELECT STRAIGHT_JOIN
                a.id,
				"min" `end`
            FROM
                objectstore a
            WHERE
                a.id={{min}}
		UNION ALL
            SELECT
                a.id,
				"min" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore c ON c.id=a.id-1
            WHERE
                c.id IS NULL AND
                a.id BETWEEN {{min}} AND {{max}}
		UNION ALL
			SELECT STRAIGHT_JOIN
                a.id,
				"max" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore b ON b.id=a.id+1
            WHERE
                b.id IS NULL AND
                a.id BETWEEN {{min}} AND {{max}}
		UNION ALL
			SELECT STRAIGHT_JOIN
                a.id,
				"max" `end`
            FROM
                objectstore a
            WHERE
                a.id={{max}}
		) a
		ORDER BY
			id,
			`end` desc
    """, {"min":settings.source.service.min, "max":settings.source.service.max})
    #RESULT COMES IN min/max PAIRS, IN ORDER
    for i, r in enumerate(ranges):
        r.index=int(floor(i/2))
    unstacked_ranges = Q.unstack(ranges, keys=["index"], column='end', value="id")

    existing_ids = set()
    for r in unstacked_ranges:
        existing_ids = existing_ids.union(
            range(r.min, r.max + 1))

    D.println("Number of ids in local: "+str(len(existing_ids)))
    return existing_ids


def copy_pushlog(settings):
    with DB(settings.destination.objectstore) as local:
        with DB(settings.source.pushlog) as prod:

            missing_revisions=local.query("""
                SELECT DISTINCT
                    o.revision
                FROM
                    {{objectstore}}.objectstore o
                LEFT JOIN
                    {{pushlog}}.changesets AS ch ON ch.revision=o.revision
                WHERE
                    o.revision is not null AND
                    ch.revision is null
            """, {
                "objectstore":SQL(settings.destination.objectstore.schema),
                "pushlog":SQL(settings.destination.pushlog.schema)
            })

            for g, batch in Q.groupby(missing_revisions, size=1000):
                pushlogs=prod.query("""
                    SELECT
                        ch.id changeset_id,
                        ch.node node,
                        ch.author author,
                        ch.branch changeset_branch,
                        ch.`desc` `desc`,
                        ch.pushlog_id,
                        pl.push_id,
                        pl.`date` `date`,
                        pl.user user,
                        pl.branch_id,
                        br.name branch_name,
                        br.uri branch_uri,
                        bm.id branch_map_id,
                        bm.alt_name
                    FROM
                        {{pushlog}}.changesets AS ch
                    LEFT JOIN
                        {{pushlog}}.pushlogs AS pl ON pl.id = ch.pushlog_id
                    LEFT JOIN
                        {{pushlog}}.branches AS br ON pl.branch_id = br.id
                    LEFT JOIN
                        {{pushlog}}.branch_map AS bm ON br.name = bm.name
                    WHERE
                        substring(ch.node, 1, 12) in {{revisions}}
                """, {
                    "pushlog":SQL(settings.source.pushlog.schema),
                    "revisions":Q.select(batch, "revision")
                })

                # ADD BRANCH_MAP. BRANCH, AND PUSHLOGS WHERE THEY DO NOT EXIST
                # NOTE THAT THESE HAVE BEEN EXPANDED, AND WE USE groupby TO PACK
                # THEM BACK DOWN TO ORIGINAL RECORDS
                local.insert_newlist(
                    settings.destination.pushlog.schema+".branch_map",
                    "id",
                    [{
                        "id":p.branch_map_id,
                        "name":p.branch_name,
                        "alt_name":p.alt_name
                    } for p, d in Q.groupby([p for p in pushlogs if p.branch_map_id is not None], ["branch_map_id", "branch_name", "alt_name"])]
                )

                local.insert_newlist(
                    settings.destination.pushlog.schema+".branches",
                    "id",
                    [{
                        "id":p.branch_id,
                        "name":p.branch_name,
                        "uri":p.branch_uri
                    } for p, d in Q.groupby(pushlogs, ["branch_id", "branch_name", "branch_uri"])]
                )

                local.insert_newlist(
                    settings.destination.pushlog.schema+".pushlogs",
                    ["push_id", "branch_id"],
                    [{
                        "id":p.pushlog_id,
                        "push_id":p.push_id,
                        "date":p.date,
                        "user":p.user,
                        "branch_id":p.branch_id
                    } for p, d in Q.groupby(pushlogs, ["pushlog_id", "push_id", "date", "user", "branch_id"])]
                )

                #NOW WE ARE SAFE TO ADD THE CHANGESETS
                local.insert_list(
                    settings.destination.pushlog.schema+".changesets",
                    [{
                        "id":p.changeset_id,
                        "node":p.node,
                        "author":p.author,
                        "branch":p.changeset_branch,
                        "desc":p.desc,
                        "pushlog_id":p.pushlog_id,
                        "revision":p.node[0:12]
                    } for p in pushlogs]
                )

            


def main(settings):
    with DB(settings.database) as db:
        try:
            functions=[functools.partial(etl, *["ETL"+str(t), db, settings]) for t in range(settings.source.service.threads)]

            if settings.source.service.max-settings.source.service.min<=100:
                #FOR SMALL NUMBERS, JUST LOAD THEM ALL AGAIN
                missing_ids=set(range(settings.source.service.min, settings.source.service.max))
            else:
                existing_ids = get_existing_ids(db)
                missing_ids=set(range(settings.source.service.min, settings.source.service.max)) - existing_ids
            settings.num_not_found=0

            with Multithread(functions) as many:
                for result in many.execute([{"id":id} for id in missing_ids]):
                    if not result:
                        settings.num_not_found+=1
                        if settings.num_not_found>100:
                            many.stop()
                            break
                    else:
                        settings.num_not_found=0

        except (KeyboardInterrupt, SystemExit):
            D.println("Shutdow Started, please be patient")
        except Exception, e:
            D.error("Unusual shutdown!", e)
        D.println("Done")

    copy_pushlog(settings)


import sys
reload(sys)
sys.setdefaultencoding("utf-8")

try:
    settings=startup.read_settings()
    settings.source.service.threads=nvl(settings.source.service.threads, 1)
    D.start(settings.debug)
    copy_pushlog(settings)
    main(settings)
except Exception, e:
    D.error("Problem", e)
finally:
    D.stop()
