# com.etendoerp.db.extended **(BETA MODULE)**

This **BETA** module extends the database functionalities of the Etendo ERP, providing advanced tools to manage complex
structures such as partitioned tables with intelligent constraint management.

## 🔧 Main Features

- **Intelligent Partitioned Table Management**: Automated constraint recreation for PostgreSQL partitioned tables
- **Smart Constraint Processing**: Detects table definition changes and manages primary/foreign key constraints
- **Backup & Safety**: Comprehensive backup system with retention policies and automatic cleanup
- **XML-Based Configuration**: Integration with Etendo's table definition system
- **Trigger Automation**: Automatic partition column population in child tables
- **Python Tools**: Command-line utilities to **partition** and **unpartition** database tables
- **Semantic search over any table**: an optional, off-by-default pgvector capability that indexes
  the columns a dictionary configuration names, keeps them up to date through a transactional
  outbox, and answers nearest-neighbour queries scoped to the session's tenant

## Optional pgvector capability

pgvector is optional and off by default. Installing this module, compiling Etendo, running
`smartbuild` or `update.database`, or starting the application never installs the PostgreSQL
extension and never creates a vector object. Turning it on is a deliberate administrator action,
and until it is taken every vector operation returns the controlled `PGVECTOR_NOT_ENABLED` error.

The API itself is entity-agnostic: a caller supplies a namespace, an external key, a numeric
vector, JSON metadata and an optional client/organization scope. No entity class is involved, and
this module implements no RAG and publishes no REST endpoint of its own — a consuming module such
as `com.etendoerp.go` does that.

### Turning it on

**Search Source → Activate Vector Indexing** is the entry point. It takes several records at a
time, so one source, a few or all of them can be turned on in a single step. Each run installs the
extension and the runtime storage once per database, and then, for each selected source, either
creates its collection or explains what is stopping it.

The check matters because a source can be broken in ways that only surface much later. One with no
content column is accepted by the dictionary and fails on every delivery; a collection created
before the provider changed holds vectors of a size the new model no longer produces. Activation
reports both instead of queueing work that can only fail, and it never repairs a mismatched
collection: making one match again means dropping it, and that deletes every vector it holds.

The action needs a database role allowed to `CREATE EXTENSION`. Running it again is harmless and
is also how a source added later finishes being set up.

### Indexing what a table already held

Triggers only capture what changes from the moment they are installed, so a source configured over
a table that already has rows indexes nothing until those rows are walked. **Search Source →
Request Reindex** asks for that walk; the scheduled process performs it in bounded chunks, keyset
by primary key, committing each one, so a table of millions of rows neither holds the session nor
fills the queue faster than delivery drains it. A source keeps a single request for its whole
life, so asking again restarts the one it has, and the button says what that costs before doing
it.

### Delivery

An enabled source enqueues its changes in `ETARC_VECTOR_OUTBOX`. The system-level background
process **Process Vector Outbox** drains it and needs no entity-specific configuration. One run
does four things in order:

1. retires events that have exhausted their provider retry limit and have been `PROCESSING` for at
   least 15 minutes,
2. requeues the remaining stale `PROCESSING` ones, which is how a run interrupted mid-flight is
   recovered,
3. delivers up to 100 pending events, grouped by source and embedded one chunk per provider
   request,
4. purges terminal events older than 30 days.

Schedule it once at System level, not once per client: the outbox is shared and each event carries
its own client and organization scope to the vector record. A run skipped because another instance
is active is reported by the scheduler as `Skipped`, not as an error.

An event ends in one of five states. `DONE` and `FAILED` are the obvious two; `SUPERSEDED` is an
event discarded because the source configuration changed after it was enqueued, so re-embedding it
would store a vector nobody asked for. `FAILED` is not retried by the scheduled process once the
retry limit is spent — **Requeue Failed Vector Events** puts those back once the cause is fixed.

### The embedding provider

`DictionaryVectorOutboxConsumer` is the generic consumer for configured sources. It resolves the
provider from the dictionary and reads the API key from the reference named in **API Key
Reference**, looked up in this order:

1. a JVM system property,
2. an environment variable,
3. `Openbravo.properties`.

The first one found wins, so a `-D` flag or an exported variable overrides the file.

**API Endpoint** is a base URL, up to and including `/v1` and no further; the path of the call is
added by the module. Leaving it empty calls OpenAI. Pointing it at an OpenAI-compatible gateway —
the Etendo LLM proxy, an Azure OpenAI deployment, a corporate gateway — makes the same
configuration work against any of them. Such a gateway serves several providers and is told which
one to use in the model name, so there **Embedding Model** takes the form `provider/model`.

Text leaves the tenant on every embedding call. **Max Input Characters** truncates a record before
it is sent, which bounds the request but also means a long record is embedded from its beginning
only.

### Searching

`VectorSearchService` is the query facade. `searchAsJson` takes one namespace or a collection of
them, embeds the query text with the configured provider, uses the collection's metric and returns
JSON matches with the external `id`, the distance, a normalised `score` in `[0, 1]`, the indexed
`fields` and the full metadata. An overload filters by score range. `searchTargetsAsJson` searches
configured target keys instead, applying each target's Display Logic filter.

Client and organization filters are mandatory and come exclusively from the active `OBContext`; a
caller cannot supply tenant scope. Searching several namespaces at once is rejected when their
provider type, model, dimension or metric differ, because distances from incompatible embedding
profiles cannot be ranked against each other.

Exact search supports cosine, L2 and inner product, and is available without any index. HNSW
creation is a separate explicit operation.

### What is not in the model

Activation creates its storage at runtime, so no vector-typed column belongs in
`src-db/database/model`. The versioned `excludeFilter.xml` keeps those runtime tables, the
generated source triggers and functions, and the pgvector extension objects out of DBSM exports.
Note that the database structure checksum does not read that file, which is why activation accepts
the structure it changed — see `VectorTriggerService.acceptDatabaseStructure`.

## 🏗️ Architecture Overview

The module follows a modular architecture with specialized components:

- **`PartitionedConstraintsHandling`**: Main coordinator (11 methods, SonarQube compliant)
- **`BackupManager`**: Database backup operations and retention management
- **`XmlTableProcessor`**: Secure XML parsing and table definition analysis
- **`ConstraintProcessor`**: PostgreSQL constraint intelligence and validation
- **`SqlBuilder`**: Dynamic SQL generation for partition-aware operations
- **`TriggerManager`**: Automated trigger creation for partition column population

*📖 For detailed architecture information, see [Architecture Guide](#-architecture-guide) below.*

---

## ▶️ Requirements

- Python 3
- PostgreSQL
- Virtualenv (`python3 -m venv`)
- DBSM Version 1.2.0 (Change this value in artifacts.list.COMPILATION.gradle file)

For the optional vector capability:

- **pgvector 0.5.0 or newer.** That is where HNSW arrived, and the module builds HNSW indexes with
  `vector_cosine_ops`, `vector_l2_ops` and `vector_ip_ops`. Nothing here uses `halfvec`,
  `sparsevec` or `binary_quantize`; `excludeFilter.xml` names them so that a server that does have
  them keeps them out of DBSM exports, and on an older one those entries simply match nothing.
- A database role allowed to run `CREATE EXTENSION`, for the activation action only.
- Developed and tested against pgvector 0.8.6 on PostgreSQL 16.

---

## ⚙️ Python Environment Setup

```bash
python3 -m venv modules/com.etendoerp.db.extended/.venv
source ./modules/com.etendoerp.db.extended/.venv/bin/activate
pip3 install pyyaml psycopg2-binary
```

## 🚀 Usage

### 📌 1. Partition a Table

⚠️ This process modifies the physical structure of the table. Use with caution and always validate backups before
execution.

#### Steps to Configure a Partitioned Table

1. Log in as System Administrator.
   Ensure you have the necessary privileges to modify system-level configurations.

2. Navigate to the Partitioned Table Config window.
   This section allows you to define how tables will be partitioned.

    1. Create a new configuration record.

    2. Select the table you want to partition.

    3. Choose the column to use for partitioning (it must be a column with a date reference).

    4. Save the configuration.

#### Apply the partitioning:

Stop the Tomcat server.

Run the partitioning script or command (details below).

```bash
python3 modules/com.etendoerp.db.extended/tool/migrate.py
./gradlew update.database -Dforce=yes smartbuild
```

The first command automatically partitions tables configured either in the data dictionary or in a YAML definition
file.  
The `update.database` task generates the structure of the partitioned tables. It is forced because the first execution
after partitioning triggers DB Source Manager to detect changes due to the new structure.

#### 🤖 Automated Constraint Management

Once tables are partitioned, the **PartitionedConstraintsHandling** module script automatically:

- **Detects Changes**: Monitors XML table definitions for modifications
- **Recreates Constraints**: Updates primary keys and foreign keys for partitioned tables
- **Creates Backups**: Automatic backup creation before making any changes
- **Manages Triggers**: Creates triggers to auto-populate partition columns in child tables
- **Handles Dependencies**: Manages foreign key relationships with external tables

The system runs automatically during module script execution and handles complex scenarios like:

- First-time partitioning (creates new constraints)
- Table definition changes (updates existing constraints)
- External foreign key references (processes dependent tables)

### 📌 2. Unpartition a Table

If you need to run `export.database` (only in development environments) and your module is under development, it's
necessary to unpartition the tables beforehand:

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

This step updates the database metadata to reflect the restored (non-partitioned) table structure, ensuring the system
continues to operate correctly.

---

## 📚 Architecture Guide

This section provides comprehensive architectural information for developers who need to understand, maintain, or extend
the partition constraint management system.

### System Purpose

The partition constraint management system addresses the complex challenges of maintaining database referential
integrity when working with PostgreSQL table partitioning in the Etendo framework. It automatically manages the
recreation of primary keys and foreign keys when table definitions change, ensuring that partitioned tables maintain
proper constraint relationships.

### Component Architecture

```
    ┌─────────────────────────────────────────────────────────────────┐
    │                    PartitionedConstraintsHandling               │
    │                        (Main Coordinator)                       │
    │  ┌─────────────────────────────────────────────────────────────┐│
    │  │ • Load configurations from ETARC_TABLE_CONFIG               ││
    │  │ • Orchestrate processing workflow                           ││
    │  │ • Handle errors and logging                                 ││
    │  │ • Report processing results                                 ││
    │  └─────────────────────────────────────────────────────────────┘│
    └─────────────────────────────────────────────────────────────────┘
          │ coordinates │ coordinates │ coordinates │ coordinates |
          ▼             ▼             ▼             ▼             ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────────┐
│BackupManager│ │XmlTableProc-│ │ConstraintPr-│ │ SqlBuilder  │ │TriggerMgr    │
│             │ │essor        │ │ocessor      │ │             │ │              │
│• Backup ops │ │• XML parse  │ │• Constraint │ │• SQL gen    │ │• Trigger mgmt│
│• Cleanup    │ │• FK detect  │ │• Table info │ │• Template   │ │• Auto-pop    │
│• Metadata   │ │• File search│ │• Validation │ │• Type map   │ │• Cleanup     │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ └──────────────┘
```

### Data Processing Flow

```
1. Configuration Loading
   ETARC_TABLE_CONFIG → loadTableConfigs() → List<Map<String, String>>

2. For Each Table Configuration:
   ┌─ XML Analysis (XmlTableProcessor)
   │  ├─ Find table XML files
   │  ├─ Check for definition changes  
   │  └─ Detect external FK references
   │
   ├─ Table Analysis (ConstraintProcessor)
   │  ├─ Check partitioning status
   │  ├─ Get primary key columns
   │  └─ Determine child table relationships
   │
   ├─ Decision Logic (PartitionedConstraintsHandling)
   │  ├─ Apply skip conditions
   │  └─ Determine processing necessity
   │
   ├─ SQL Generation (SqlBuilder + ConstraintProcessor)
   │  ├─ Generate ALTER statements
   │  ├─ Generate constraint SQL
   │  └─ Include trigger SQL if needed
   │
   └─ Execution (BackupManager)
      ├─ Create table backup
      ├─ Execute SQL statements
      └─ Log results
```

### Component Responsibilities

#### 1. PartitionedConstraintsHandling (Main Coordinator)

- **Responsibility**: Orchestrates the entire constraint management process
- **Methods**: 11 methods (SonarQube compliant)
- **Pattern**: Coordinator Pattern with Template Method structure

#### 2. BackupManager (Data Protection)

- **Responsibility**: Ensures data safety through comprehensive backup strategy
- **Features**: Automatic infrastructure, retention policies, metadata tracking
- **Database Objects**: `etarc_backups` schema, `backup_metadata` tables

#### 3. XmlTableProcessor (Definition Analysis)

- **Responsibility**: Analyzes XML table definitions and detects changes
- **Security**: XXE attack protection, secure parsing
- **Locations**: Searches across module directories for table definitions

#### 4. ConstraintProcessor (Constraint Intelligence)

- **Responsibility**: Central intelligence for constraint analysis and SQL coordination
- **Features**: PostgreSQL partitioning detection, primary key analysis, foreign key discovery

#### 5. SqlBuilder (SQL Generation)

- **Responsibility**: Generates precise SQL statements for constraint modifications
- **Strategy**: Template-based SQL with partition-aware logic
- **Types**: Primary key operations, foreign key operations, ALTER statements

#### 6. TriggerManager (Automation)

- **Responsibility**: Creates triggers for automatic partition column population
- **Pattern**: Analyzes XML → Creates PL/pgSQL functions → Implements triggers
- **Naming**: `etarc_populate_<tablename>_<partition_field>()` functions

### Processing Decision Matrix

| Condition                                | Action  | Reason                       |
|------------------------------------------|---------|------------------------------|
| Configuration incomplete                 | Skip    | Missing required information |
| No XML changes + Not first partition run | Skip    | No changes to process        |
| XML changes detected                     | Process | Table definition modified    |
| First partition run                      | Process | Constraints need recreation  |
| External FK references found             | Process | Other tables affected        |

### PostgreSQL Partitioning Specifics

#### Key Differences

- **Partitioned PK**: Must include both natural PK and partition key
- **Non-Partitioned PK**: Uses only the natural primary key
- **Foreign Keys**: Must reference all columns in target table's primary key

#### Example SQL Generation

```sql
-- Non-partitioned PK
ALTER TABLE table_name ADD CONSTRAINT pk_name PRIMARY KEY (id);

-- Partitioned PK  
ALTER TABLE table_name ADD CONSTRAINT pk_name PRIMARY KEY (id, partition_col);

-- Trigger Function for Auto-Population
CREATE OR REPLACE FUNCTION etarc_populate_tablename_partcol()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.partition_col IS NULL THEN
        SELECT partition_col INTO NEW.partition_col
        FROM parent_table
        WHERE parent_table.pk = NEW.fk_col;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### Configuration Schema

#### ETARC_TABLE_CONFIG Structure

```sql
CREATE TABLE ETARC_TABLE_CONFIG (
    AD_TABLE_ID VARCHAR(32),     -- References AD_TABLE
    AD_COLUMN_ID VARCHAR(32)     -- References AD_COLUMN (partition column)
);
```

#### Resolved Configuration

```java
Map<String, String> config = {
    "tableName":"C_ORDER",           // From AD_TABLE.TABLENAME
    "columnName":"DATEORDERED",      // From AD_COLUMN.COLUMNNAME  
    "pkColumnName":"C_ORDER_ID"      // From AD_COLUMN where ISKEY='Y'
    }
```

### Error Handling & Safety

#### Defensive Programming

- Safe existence checks for database objects
- Graceful degradation on non-critical failures
- Exception isolation between table processing

#### Backup Strategy

- Always backup before modifications
- Continue processing even if backup fails
- Retention policy: 5 backups per table, 7 days max age

#### Logging Strategy

- **INFO**: Normal operations, skip reasons, processing summaries
- **WARN**: Recoverable issues, incomplete configurations
- **ERROR**: Processing failures, SQL execution errors

### Performance Considerations

#### Optimization Features

- Quick partitioning check to skip unnecessary processing
- Batch processing with timing measurements
- Efficient XML file caching and parsing
- Per-table timing and summary reporting

### Extension Points

#### Adding New Constraint Types

1. Extend `SqlBuilder` with new templates
2. Update `ConstraintProcessor` analysis logic
3. Add corresponding methods to `FkContext` interface

#### Custom Backup Strategies

1. Extend `BackupManager` with new backup methods
2. Implement custom retention policies
3. Add backup verification logic

### Development Guidelines

#### For Future Developers ("For Those Who Come After...")

When working with this system:

1. **Follow Established Patterns**: Each component has a single responsibility
2. **Maintain Separation of Concerns**: Don't mix backup logic with SQL generation
3. **Update Documentation**: Keep JavaDoc and this guide current
4. **Add Appropriate Tests**: Unit tests for components, integration tests for workflows
5. **Consider Backward Compatibility**: Changes should not break existing configurations

#### Testing Strategies

- **Unit Testing**: Mock `ConnectionProvider` for database independence
- **Integration Testing**: Test with real PostgreSQL database
- **Performance Testing**: Validate large dataset processing

#### Common Troubleshooting

1. **Missing XML Files**: Check module structure and file paths
2. **Constraint Recreation Failures**: Verify table exists and check PostgreSQL logs
3. **Backup Operations Failing**: Verify database permissions and disk space
4. **Performance Issues**: Monitor connection pool and check for large table scans

## 📖 Additional Resources

- [ARCHITECTURE_GUIDE.md](ARCHITECTURE_GUIDE.md) — the partitioning internals in more depth.
- Administrator and consultant documentation lives at
  [docs.etendo.software](https://docs.etendo.software), not in this repository.
- The history of the module is its git history; this file describes what the module does now.

## 🏷️ Version Information

- **Current Version**: BETA
- **Architecture**: Modular design with 6 specialized components
- **SonarQube Compliance**: ✅ Main class reduced from 76 to 11 methods
- **Code Quality**: Comprehensive JavaDoc documentation and architectural guides
- **Last Major Refactoring**: ETP-2450 (September 2025)

## 🤝 Contributing

When contributing to this module:

1. **Review Architecture**: Read the [Architecture Guide](ARCHITECTURE_GUIDE.md) first
2. **Follow Patterns**: Maintain the established component separation
3. **Update Documentation**: Keep JavaDoc and guides current
4. **Test Thoroughly**: Include unit and integration tests
5. **Consider Impact**: Ensure backward compatibility

## 📞 Support & Contact

For questions about this module:

- **Technical Issues**: Review the troubleshooting section above
- **Architecture Questions**: Consult the [Architecture Guide](ARCHITECTURE_GUIDE.md)
- **Documentation**: All classes have comprehensive JavaDoc

---

*This module was designed with future developers in mind. The comprehensive documentation and modular architecture
should provide everything needed to understand, maintain, and extend the system effectively.*

**"For those who come after..."**
