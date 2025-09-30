# Partition Constraint Management - Architecture Guide

## Overview

This document provides a comprehensive architectural overview of the partition constraint management system, designed
for developers who need to understand, maintain, or extend this functionality.

## System Purpose

The partition constraint management system addresses the complex challenges of maintaining database referential
integrity when working with PostgreSQL table partitioning in the Etendo framework. It automatically manages the
recreation of primary keys and foreign keys when table definitions change, ensuring that partitioned tables maintain
proper constraint relationships.

## Core Architecture

### Component Diagram

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

### Data Flow

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

## Component Details

### 1. PartitionedConstraintsHandling (Main Coordinator)

**Responsibility**: Orchestrates the entire constraint management process.

**Key Methods**:

- `execute()`: Main entry point, coordinates entire workflow
- `processTableConfig()`: Processes individual table configurations
- `loadTableConfigs()`: Loads configurations from database
- `shouldSkipTable()`: Implements skip logic

**Design Patterns**:

- **Coordinator Pattern**: Orchestrates multiple specialized components
- **Template Method**: Defines processing algorithm structure

### 2. BackupManager (Data Protection)

**Responsibility**: Ensures data safety through comprehensive backup strategy.

**Key Features**:

- Automatic backup infrastructure creation
- Retention policy management (5 backups per table, 7 days max age)
- Metadata tracking for all backup operations
- Safe SQL execution with rollback capabilities

**Database Objects**:

- `etarc_backups` schema
- `backup_metadata` table
- `processing_metadata` table
- Individual backup tables: `backup_<tablename>_<timestamp>`

### 3. XmlTableProcessor (Definition Analysis)

**Responsibility**: Analyzes XML table definitions and detects changes.

**Security Features**:

- XXE (XML External Entity) attack protection
- DOCTYPE declaration blocking
- Entity expansion prevention
- Secure processing mode

**Search Locations**:

```
src-db/database/model/tables/
src-db/database/model/modifiedTables/
build/etendo/modules/
modules/
modules_core/
```

### 4. ConstraintProcessor (Constraint Intelligence)

**Responsibility**: Central intelligence for constraint analysis and SQL coordination.

**Key Capabilities**:

- PostgreSQL partitioning detection
- Primary key column analysis
- Foreign key relationship discovery
- Child table identification
- Constraint existence validation

**PostgreSQL Specifics**:

- Partitioned table constraint requirements
- Primary key must include partition key
- Foreign key cascading considerations

### 5. SqlBuilder (SQL Generation)

**Responsibility**: Generates precise SQL statements for constraint modifications.

**Template Categories**:

- Primary key operations (DROP/ADD)
- Foreign key operations (partitioned vs simple)
- Helper column management
- ALTER table statements

**Partitioning Logic**:

```sql
-- Non-partitioned PK
ALTER TABLE table_name ADD CONSTRAINT pk_name PRIMARY KEY (id);

-- Partitioned PK  
ALTER TABLE table_name ADD CONSTRAINT pk_name PRIMARY KEY (id, partition_col);
```

### 6. TriggerManager (Automation)

**Responsibility**: Creates triggers for automatic partition column population.

**Trigger Strategy**:

- Analyzes child table foreign keys
- Creates PL/pgSQL functions for column population
- Implements BEFORE INSERT/UPDATE triggers
- Handles trigger cleanup

**Generated SQL Pattern**:

```sql
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

## Processing Decision Matrix

| Condition                                | Action  | Reason                       |
|------------------------------------------|---------|------------------------------|
| Configuration incomplete                 | Skip    | Missing required information |
| No XML changes + Not first partition run | Skip    | No changes to process        |
| XML changes detected                     | Process | Table definition modified    |
| First partition run                      | Process | Constraints need recreation  |
| External FK references found             | Process | Other tables affected        |

## Error Handling Strategy

### Defensive Programming

- Safe existence checks for database objects
- Graceful degradation on non-critical failures
- Exception isolation between table processing

### Logging Strategy

- **INFO**: Normal operations, skip reasons, processing summaries
- **WARN**: Recoverable issues, incomplete configurations
- **ERROR**: Processing failures, SQL execution errors

### Backup Strategy

- Always backup before modifications
- Continue processing even if backup fails
- Maintain backup metadata for recovery

## Configuration Schema

### ETARC_TABLE_CONFIG Structure

```sql
CREATE TABLE ETARC_TABLE_CONFIG (
    AD_TABLE_ID VARCHAR(32),     -- References AD_TABLE
    AD_COLUMN_ID VARCHAR(32)     -- References AD_COLUMN (partition column)
);
```

### Resolved Configuration Map

```java
Map<String, String> config = {
    "tableName":"C_ORDER",           // From AD_TABLE.TABLENAME
    "columnName":"DATEORDERED",      // From AD_COLUMN.COLUMNNAME  
    "pkColumnName":"C_ORDER_ID"      // From AD_COLUMN where ISKEY='Y'
    }
```

## Performance Considerations

### Optimization Strategies

- Quick partitioning check to skip unnecessary processing
- Batch processing with timing measurements
- Efficient XML file caching and parsing
- Connection pooling through ConnectionProvider

### Timing and Monitoring

- Per-table processing time measurement
- Processing summary reporting
- Progress logging for large datasets

## Extension Points

### Adding New Constraint Types

1. Extend `SqlBuilder` with new templates
2. Update `ConstraintProcessor` analysis logic
3. Add corresponding methods to `FkContext` interface

### Custom Backup Strategies

1. Extend `BackupManager` with new backup methods
2. Implement custom retention policies
3. Add backup verification logic

### Additional XML Analysis

1. Extend `XmlTableProcessor` with new parsing methods
2. Add support for additional XML element types
3. Implement custom change detection logic

## Testing Strategies

### Unit Testing Approach

- Mock `ConnectionProvider` for database independence
- Test individual component logic in isolation
- Validate SQL generation with known inputs

### Integration Testing

- Test with real PostgreSQL database
- Validate backup and recovery operations
- Test partitioned and non-partitioned scenarios

### Performance Testing

- Large dataset processing validation
- Memory usage monitoring
- SQL execution timing analysis

## Troubleshooting Guide

### Common Issues

1. **Missing XML Files**
    - Check module structure and file paths
    - Verify source path configuration
    - Review search directory permissions

2. **Constraint Recreation Failures**
    - Verify table exists and is accessible
    - Check for blocking transactions
    - Review PostgreSQL logs for detailed errors

3. **Backup Operations Failing**
    - Verify database permissions
    - Check disk space availability
    - Review backup schema permissions

4. **Performance Issues**
    - Monitor database connection pool
    - Check for large table scan operations
    - Review XML file system performance

### Debug Logging

Enable debug logging to trace processing flow:

```xml

<logger name="com.etendoerp.db.extended.utils" level="DEBUG"/>
```

## Future Enhancements

### Planned Improvements

- Parallel processing for large table sets
- Enhanced backup compression
- Real-time constraint validation
- Performance metrics dashboard

### Extensibility Roadmap

- Plugin architecture for custom processors
- REST API for external integration
- Configuration management UI
- Automated testing framework

---

## For Those Who Come After...

This system was designed with maintainability and extensibility in mind. Each component has a single, well-defined
responsibility and clear interfaces. The comprehensive JavaDoc documentation and this architectural guide should provide
you with everything needed to understand, modify, and extend this system.

When making changes:

1. Follow the established patterns
2. Maintain the separation of concerns
3. Update documentation accordingly
4. Add appropriate tests
5. Consider backward compatibility

The refactoring from a 76-method monolithic class to this modular architecture was done to improve maintainability and
comply with code quality standards. The new structure makes the system more testable, understandable, and extensible.
