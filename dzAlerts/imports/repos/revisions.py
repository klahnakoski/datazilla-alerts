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


class Revision(object):
    def __init__(self, branch, changeset, index=None, push=None, parents=None, children=None, files=None):
        self.branch = wrap(branch)
        self.changeset = wrap(changeset)
        self.index = index
        self.push = push
        self.parents = parents
        self.children = children
        self.files = files

    def __hash__(self):
        return hash((self.branch.name.lower(), self.changeset.id[:12]))

    def __eq__(self, other):
        return (self.branch.name.lower(), self.changeset.id[:12]) == (other.branch.name.lower(), other.changeset.id[:12])