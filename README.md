# com.etendoerp.db.extended

This module extends the database functionalities of the Etendo ERP, providing advanced tools to manage complex structures such as partitioned tables.

## 🔧 Main Features

- Tools to **partition** and **unpartition** PostgreSQL database tables.

---

## ▶️ Requirements

- Python 3
- PostgreSQL
- Virtualenv (`python3 -m venv`)

---

## ⚙️ Python Environment Setup

```bash
python3 -m venv modules/com.etendoerp.db.extended/.venv
source ./modules/com.etendoerp.db.extended/.venv/bin/activate
pip3 install pyyaml psycopg2-binary
```

## 🚀 Usage

### 📌 1. Partition a Table
⚠️ This process modifies the physical structure of the table. Use with caution and always validate backups before execution.

```bash
python3 modules/com.etendoerp.db.extended/tool/migrate.py
./gradlew update.database -Dforce=yes smartbuild
```

The first command automatically partitions tables configured either in the data dictionary or in a YAML definition file.  
The `update.database` task generates the structure of the partitioned tables. It is forced because the first execution after partitioning triggers DB Source Manager to detect changes due to the new structure.

### 📌 2. Unpartition a Table
If you need to run `export.database` (only in development environments) and your module is under development, it's necessary to unpartition the tables beforehand:

```bash
python3 modules/com.etendoerp.db.extended/tool/unpartition.py "table_name"
```

For example:

```bash
python3 modules/com.etendoerp.db.extended/tool/unpartition.py "etpur_archive"
```

This will restore the table to its original (non-partitioned) structure, allowing the export to complete successfully.
