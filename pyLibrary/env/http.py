# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# MIMICS THE requests API (http://docs.python-requests.org/en/latest/)
# WITH ADDED default_headers THAT CAN BE SET USING pyLibrary.debugs.settings
# EG
# {"debug.constants":{
# "pyLibrary.env.http.default_headers={
# "From":"klahnakoski@mozilla.com"
#     }
# }}


from __future__ import unicode_literals
from __future__ import division
import StringIO
import gzip

from requests import sessions, Response

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, nvl
from pyLibrary.env.big_data import safe_size, MAX_STRING_SIZE, CompressedLines, LazyLines, GzipLines, ZipfileLines


FILE_SIZE_LIMIT = 100 * 1024 * 1024
MIN_READ_SIZE = 8 * 1024
default_headers = Dict()  # TODO: MAKE THIS VARIABLE A SPECIAL TYPE OF EXPECTED MODULE PARAMETER SO IT COMPLAINS IF NOT SET
default_timeout = 600

_warning_sent = False


def request(method, url, **kwargs):
    global _warning_sent
    if not default_headers and not _warning_sent:
        _warning_sent = True
        Log.warning("The pyLibrary.env.http module was meant to add extra "
                    "default headers to all requests, specifically the 'From' "
                    "header with a URL to the project, or email of developer. "
                    "Use the constants.set() function to set pyLibrary.env.http.default_headers"
        )

    session = sessions.Session()
    session.headers.update(default_headers)

    kwargs['timeout'] = nvl(kwargs.get('timeout'), default_timeout)

    if len(nvl(kwargs.get("data"))) > 1000:
        compressed = convert.bytes2zip(kwargs["data"])
        kwargs["headers"]['content-encoding'] = 'gzip'
        kwargs["data"] = compressed
        return session.request(method=method, url=url, **kwargs)
    else:
        return session.request(method=method, url=url, **kwargs)


def get(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    kwargs["stream"] = True
    return HttpResponse(request('get', url, **kwargs))


def options(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    kwargs["stream"] = True
    return HttpResponse(request('options', url, **kwargs))


def head(url, **kwargs):
    kwargs.setdefault('allow_redirects', False)
    kwargs["stream"] = True
    return HttpResponse(request('head', url, **kwargs))


def post(url, **kwargs):
    kwargs["stream"] = True
    return HttpResponse(request('post', url, **kwargs))


def put(url, **kwargs):
    return HttpResponse(request('put', url, **kwargs))


def patch(url, **kwargs):
    kwargs["stream"] = True
    return HttpResponse(request('patch', url, **kwargs))


def delete(url, **kwargs):
    kwargs["stream"] = True
    return HttpResponse(request('delete', url, **kwargs))


class HttpResponse(Response):
    def __new__(cls, resp):
        resp.__class__ = HttpResponse
        return resp

    def __init__(self, resp):
        pass
        self._cached_content = None

    @property
    def all_content(self):
        # Response.content WILL LEAK MEMORY (?BECAUSE OF PYPY"S POOR HANDLING OF GENERATORS?)
        # THE TIGHT, SIMPLE, LOOP TO FILL blocks PREVENTS THAT LEAK
        if self._cached_content is None:
            def read(size):
                if self.raw._fp.fp is not None:
                    return self.raw.read(amt=size, decode_content=True)
                else:
                    self.close()
                    return None

            self._cached_content = safe_size(Dict(read=read))

        if hasattr(self._cached_content, "read"):
            self._cached_content.seek(0)

        return self._cached_content

    @property
    def all_lines(self):
        try:
            content = self.raw.read(decode_content=False)
            if self.headers.get('content-encoding') == 'gzip':
                return CompressedLines(content)
            elif self.headers.get('content-type') == 'application/zip':
                return ZipfileLines(content)
            else:
                return convert.utf82unicode(content).split("\n")
        except Exception, e:
            Log.error("Not JSON", e)
        finally:
            self.close()
