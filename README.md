## Execute
python3 -m venv modules/com.etendoerp.archiving/.venv
source ./modules/com.etendoerp.archiving/.venv/bin/activate
pip3 install pyyaml psycopg2-binary

python3 modules/com.etendoerp.archiving/tool/migrate.py 

# com.etendoerp.archiving
