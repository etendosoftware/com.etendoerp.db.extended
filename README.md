# com.etendoerp.db.extended **(BETA MODULE)**

This **BETA** module extends the database functionalities of the Etendo ERP, providing advanced tools to manage complex structures such as partitioned tables.

## 🔧 Main Features

- **Automated Table Partitioning**: Tools to partition PostgreSQL database tables by date ranges with intelligent data migration
- **Data Preservation**: Automatic backup and restore mechanisms to prevent data loss during partitioning operations
- **Performance Optimization**: Enhanced database performance for large tables through partitioning strategies
- **Constraint Management**: Intelligent handling of primary keys, foreign keys, and other constraints in partitioned environments
- **Unpartitioning Support**: Safe restoration of tables to their original non-partitioned structure

---

## ▶️ Requirements

- Python 3
- PostgreSQL
- Virtualenv (`python3 -m venv`)
- DBSM Version 1.2.0 (Change this value in artifacts.list.COMPILATION.gradle file)

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

#### Steps to Configure a Partitioned Table

1. Log in as System Administrator.
    Ensure you have the necessary privileges to modify system-level configurations.

2. Navigate to the Partitioned Table Config window.
    This section allows you to define how tables will be partitioned.

    1. Create a new configuration record.

    2. Select the table you want to partition.

    3. Choose the column to use for partitioning (it must be a column with a date reference).

    4. Save the configuration.

#### Apply the partitioning

Stop the Tomcat server.

Run the partitioning script or command (details below).

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

#### 🔁 Final Step After Unpartitioning

To ensure consistency and proper functionality after unpartitioning a table, you must regenerate the database structure:

```bash
./gradlew update.database -Dforce=yes smartbuild
```

This step updates the database metadata to reflect the restored (non-partitioned) table structure, ensuring the system continues to operate correctly.

---

## ⚠️ Known Issues

### 1. Module Installation/Uninstallation with Foreign Key Dependencies

**Issue**: Cannot install or uninstall modules that create tables with foreign key references (FK) to partitioned tables.

**Workaround**:

1. Unpartition the referenced table before module installation/uninstallation:

   ```bash
   python3 modules/com.etendoerp.db.extended/tool/unpartition.py "table_name"
   ```

2. Install or uninstall the module as needed
3. Re-partition the table after the operation:

   ```bash
   python3 modules/com.etendoerp.db.extended/tool/migrate.py
   ./gradlew update.database -Dforce=yes smartbuild
   ```

### 2. Index Issues After Partitioning

**Issue**: Some indexes may not be properly created or maintained after the partitioning process.

**Workaround**: Re-run the database update command to regenerate indexes:

```bash
./gradlew update.database -Dforce=yes
```

---

## 📋 Important Notes

- This is a **BETA** module. Always test thoroughly in development environments before using in production
- Always create full database backups before performing partitioning operations
- The module automatically creates backup tables in the `etarc_backup` schema during operations
- Partitioning is optimized for tables with date-based columns and is most effective for large datasets (>1M rows)
