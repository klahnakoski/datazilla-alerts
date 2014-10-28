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

class Push(object):
    def __init__(self, id, branch, date, user):
        self.id = id
        self.branch = branch
        self.date = date
        self.user = user
        self.changesets = None
