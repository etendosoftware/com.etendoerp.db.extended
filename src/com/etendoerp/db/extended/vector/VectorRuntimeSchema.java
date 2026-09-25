/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;

import org.openbravo.database.ConnectionProvider;

/**
 * The schema the runtime vector storage lives in, and why it is not the application's own.
 *
 * <p>{@code ad_db_modified} hashes the database structure to decide whether an instance has local
 * changes, and {@code update.database} refuses to run when it does. Its queries for tables,
 * columns, indexes, functions and materialized views are all restricted to
 * {@code current_schema()}, so anything created outside it is invisible to that checksum. Measured
 * on PostgreSQL 16: a table or a function in another schema leaves the verdict at {@code N}, the
 * same objects in the application schema move it to {@code Y}.</p>
 *
 * <p>These three tables are created by an administrator pressing a button, long after the last
 * update stamped the checksum. Keeping them here means that act no longer has to be blessed. It
 * does not cover the capture triggers: those live on the application's own tables, and the trigger
 * query is the one part of the checksum that is not restricted by schema.</p>
 */
final class VectorRuntimeSchema {

  static final String SCHEMA = "etarc_vector";
  static final String ACTIVATION = SCHEMA + ".etarc_vector_activation";
  static final String COLLECTION = SCHEMA + ".etarc_vector_collection";
  static final String RECORD = SCHEMA + ".etarc_vector_record";

  /**
   * Creates the schema and moves storage an earlier version left in the application schema.
   *
   * <p>The move is what makes this safe to deploy onto an instance that already indexed something:
   * {@code ALTER TABLE ... SET SCHEMA} carries the rows, the indexes and the foreign key with it,
   * so the vectors already stored stay stored. Creating the tables in the new schema without
   * moving the old ones would leave the instance with two sets and an index that silently answers
   * nothing.</p>
   */
  private static final String ENSURE_SQL =
      "DO $ensure$ "
          + "DECLARE t text; "
          + "BEGIN "
          + "  EXECUTE 'CREATE SCHEMA IF NOT EXISTS " + SCHEMA + "'; "
          + "  FOREACH t IN ARRAY ARRAY['etarc_vector_activation', 'etarc_vector_collection', "
          + "                           'etarc_vector_record'] LOOP "
          + "    IF EXISTS (SELECT 1 FROM pg_tables "
          + "                WHERE schemaname = current_schema() AND tablename = t) "
          + "       AND NOT EXISTS (SELECT 1 FROM pg_tables "
          + "                        WHERE schemaname = '" + SCHEMA + "' AND tablename = t) THEN "
          + "      EXECUTE format('ALTER TABLE %I.%I SET SCHEMA " + SCHEMA + "', current_schema(), t); "
          + "    END IF; "
          + "  END LOOP; "
          + "END "
          + "$ensure$";

  private VectorRuntimeSchema() {
  }

  /**
   * Makes sure the schema exists and holds the storage, moving it there if it is still elsewhere.
   *
   * @param cp
   *     connection the schema is created with
   * @throws Exception
   *     if the schema cannot be created or the existing storage cannot be moved
   */
  static void ensure(ConnectionProvider cp) throws Exception {
    try (PreparedStatement statement = cp.getPreparedStatement(ENSURE_SQL)) {
      statement.executeUpdate();
    }
  }
}
