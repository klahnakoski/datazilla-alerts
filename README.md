
dzAlerts
========
Detect regressions and improvments in peformance data ([more](https://wiki.mozilla.org/Auto-tools/Projects/Alerts))

Requirements
------------
  * PyPy >= 2.1.0 using Python 2.7  (PyPy version must have fixed [issue1392](https://bugs.pypy.org/issue1392) applied on June2013)
  * A MySQL/Maria database
  * An ElasticSearch (v0.90.x) cluster to hold the test results

Installation
------------
Python and SetupTools are required.  It is best you install on Linux, but if
you do install on Windows please [follow instructions to get these installed](https://github.com/klahnakoski/pyLibrary#windows-7-install-instructions-for-python).
When done, installation is easy:

    git clone https://github.com/klahnakoski/datazilla-alerts.git

then install requirements:

    cd datazilla-alerts
    pip install -r requirements.txt

Choose a Branch
---------------
To balance stability with features, each deployment has its own branch, with
occasional cross-pollination so they do not diverge.

  * **dev** - current development
  * **b2g** - b2g staging server
  * **eideticker** - eideticker staging server
  * **talos** - talos staging server
  * **master** - eventual production deployment

Setup Alert Database
--------------------
You can use [setup.py](tests/resources/python/setup.py) to setup the alerts database,

    python tests/resources/python/setup.py --settings=tests/resources/settings/setup_settings.json

or you may simply execute the various sql files found in [resources/schema](resources/schema)


Running Alerts
--------------

    https://github.com/klahnakoski/datazilla-alerts/blob/master/resources/scripts/<type>-alerts.bat


