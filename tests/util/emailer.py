
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
from dzAlerts.util.struct import StructList


class Emailer:
#dummy emailer

    def __init__(self, settings):
        self.sent=StructList()


    def send_email(self, **args):
        self.sent.append(args)      #SIMPLY RECORD THE CALL FOR LATER VERIFICATION