SET DATAZILLA_DATABASE_HOST=klahnakoski-es.corp.tor1.mozilla.com
SET DATAZILLA_DATABASE_PORT=3306
SET DATAZILLA_DATABASE_NAME=datazilla
SET DATAZILLA_DATABASE_USER=root
SET DATAZILLA_DATABASE_PASSWORD=manager
SET DATAZILLA_RO_DATABASE_USER=root
SET DATAZILLA_RO_DATABASE_PASSWORD=manager
python manage.py transfer_production_data --host datazilla.mozilla.org --prod_project talos --dev_project ekyle --days_ago 30