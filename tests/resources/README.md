Setup Database
--------------

You may want to setup a development or staging server to review-and-debug the ongoing behaviour of the alerts.
The Datazilla schema must be copied to a development database before you begin, and some alterations made to simulate
the Datazilla main program.

  1. ```Upgrade perftest.sql``` - Allow nulls in columns we do not use, and some convenient functions. Run this first.
  2. ```Upgrade objectstore.sql``` - for enhancing objectstore
  3. ```Add test_data_all_dimensions.sql``` - if you need the cube defined in your schema

