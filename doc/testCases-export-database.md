# ✅ Test Cases: Partitioned Table Validation in export.database
## 🔧 Context

The task ./gradlew export.database must validate whether a module under development contains partitioned tables. If so, it should throw an informative error and abort the task.

---

## ✅ Case 1: Export without partitioned tables

#### Name: Export without partitioned tables in any module  
#### Objective: Verify that export.database works correctly when there are no partitioned tables.  
#### Precondition:

No table in the database is partitioned.

There is at least one module under development.

#### Steps:

Run ./gradlew export.database

#### Expected Result:

The task finishes successfully.

No message related to partitioned tables is shown.

---

## ✅ Case 2: Export with partitioned table in non-development module

#### Name: Export with partitioned tables in non-development modules

#### Objective: Verify that export.database does not fail if the partitioned tables belong to modules not under development.

#### Precondition:

At least one table is partitioned.

That table belongs to a module that is not under development.

#### Steps:

Run ./gradlew export.database.

#### Expected Result:

The task finishes successfully.

No partition-related error message is shown.

---

## ❌ Case 3: Export with partitioned table in module under development

#### Name: Export with partitioned tables in module under development

#### Objective: Verify that the export.database task fails when there are partitioned tables in modules under development.

#### Precondition:

At least one table is partitioned.

That table belongs to a module under development.

#### Steps:

Correctly partition the table (etpur_archive).

Ensure the corresponding module is under development.

Run ./gradlew export.database.

#### Expected Result:

The task fails.

An error message like the following is displayed:

```bash
[ERROR] =============================================================
[ERROR] The module contains partitioned table: [etpur_archive]
[ERROR] Please unpartition the table and try again.
[ERROR] HINT: Run this command:
[ERROR] python3 modules/com.etendoerp.db.extended/tool/unpartition.py "etpur_archive"
[ERROR] =============================================================
```

---

## 🔁 Case 4: Unpartition table and retry export

#### Name: Export after unpartitioning the table

#### Objective: Verify that after unpartitioning a previously failing table, the export works correctly again.

#### Precondition:

A partitioned table caused a failure as in case 3.

The table belongs to a module under development.

#### Steps:

Run the unpartitioning script:

```bash
python3 modules/com.etendoerp.db.extended/tool/unpartition.py "etpur_archive"
```

Run ./gradlew export.database.

#### Expected Result:

The task finishes successfully.

No partition error is shown.

---

## ❌ Case 5: Export structural changes in module with partitioned tables

#### Name: Export for structural changes fails if there are partitioned tables

#### Objective: Verify that export.database fails even if the goal is to export only structural changes, when the module under development contains partitioned tables.

#### Precondition:

The module is under development.

It has at least one partitioned table.

The structure was modified (e.g., a column was added).

#### Steps:

Run ./gradlew export.database.

#### Expected Result:

The task fails with the same error message for partitioned tables.

No structure export is generated.

---

## ❌ Case 6 (optional): Data export with partitioned tables in module under development

#### Name: Data export fails if there are partitioned tables in module under development

#### Objective: Verify that export fails if the goal is to export only data (e.g., configuration records), but the module contains partitioned tables.

#### Precondition:

The module is under development.

It has at least one partitioned table.

Test or configuration records (that are exportable) were loaded.

#### Steps:

Run ./gradlew export.database.

#### Expected Result:

The task fails with the corresponding error message.

The records are not exported.
