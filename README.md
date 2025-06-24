## Execute
python3 -m venv modules/com.etendoerp.db.extended/.venv
source ./modules/com.etendoerp.db.extended/.venv/bin/activate
pip3 install pyyaml psycopg2-binary

python3 modules/com.etendoerp.db.extended/tool/migrate.py 

# com.etendoerp.db.extended
