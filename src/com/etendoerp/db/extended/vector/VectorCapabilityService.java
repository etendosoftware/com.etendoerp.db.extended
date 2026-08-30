/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing rights
 * and limitations under the License.
 * All portions are Copyright © 2021–2025 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */

package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.openbravo.database.ConnectionProvider;

/**
 * Inspects pgvector through PostgreSQL catalogs without changing database state.
 *
 * <p>This service deliberately performs no extension installation and does not execute DDL. Activation
 * is owned by a separate explicit lifecycle introduced after disabled-mode verification.</p>
 */
public class VectorCapabilityService {
  static final String VECTOR_EXTENSION = "vector";
  static final String CAPABILITY_SQL =
      "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = ?) AS available, "
          + "EXISTS (SELECT 1 FROM pg_extension WHERE extname = ?) AS installed";

  private final ConnectionProvider connectionProvider;

  public VectorCapabilityService(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  /**
   * Returns the extension state for the current database using read-only catalog queries only.
   *
   * @return the detected capability or a safe failed-state diagnostic when inspection cannot complete
   */
  public VectorCapability inspect() {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(CAPABILITY_SQL)) {
      statement.setString(1, VECTOR_EXTENSION);
      statement.setString(2, VECTOR_EXTENSION);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return new VectorCapability(VectorCapabilityState.FAILED,
              "Could not inspect the PostgreSQL pgvector capability.");
        }
        if (resultSet.getBoolean("installed")) {
          return new VectorCapability(VectorCapabilityState.ACTIVE,
              "The pgvector extension is installed in this database.");
        }
        if (resultSet.getBoolean("available")) {
          return new VectorCapability(VectorCapabilityState.AVAILABLE,
              "The pgvector extension is available but has not been activated in this database.");
        }
        return new VectorCapability(VectorCapabilityState.UNAVAILABLE,
            "The PostgreSQL server does not provide the pgvector extension.");
      }
    } catch (Exception exception) {
      return new VectorCapability(VectorCapabilityState.FAILED,
          "Could not inspect the PostgreSQL pgvector capability.");
    }
  }
}
