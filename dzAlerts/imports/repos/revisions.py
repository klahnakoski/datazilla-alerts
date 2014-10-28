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
class Revision(object):
   def __init__(self, branch, changeset, index=None):
       self.branch = branch
       self.changeset = changeset
       self.index = index

   def __hash__(self):
       return hash((self.branch.name, self.changeset.id))

   def __eq__(self, other):
       return (self.branch.name, self.changeset.id) == (other.branch.name, other.changeset.id)
