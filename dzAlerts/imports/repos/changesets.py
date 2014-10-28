# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from pyLibrary.structs.wraps import wrap


class Changeset(object):
    def __init__(self, id, push, **kwargs):
        self.id = id
        self.push = push
        kwargs=wrap(kwargs)
        self.files=kwargs.files
        self.tags=kwargs.tags
        self.author=kwargs.author
        self.desciption=kwargs.description
        self.files=kwargs.files

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

