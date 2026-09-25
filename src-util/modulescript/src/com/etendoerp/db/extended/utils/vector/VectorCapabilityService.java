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

package com.etendoerp.db.extended.utils.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Inspects pgvector through PostgreSQL catalogs without changing database state.
 *
 * <p>This service deliberately performs no extension installation and does not execute DDL. Activation
 * is owned by a separate explicit lifecycle introduced after disabled-mode verification.</p>
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.VectorCapabilityService} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 */
public class VectorCapabilityService {
  private static final Logger log = LogManager.getLogger();

  static final String VECTOR_EXTENSION = "vector";
  static final String CAPABILITY_SQL =
      "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = ?) AS available, "
          + "EXISTS (SELECT 1 FROM pg_extension WHERE extname = ?) AS installed";

  private final ConnectionProvider connectionProvider;
  private VectorCapability activeCapability;

  public VectorCapabilityService(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  /**
   * Returns the extension state for the current database using read-only catalog queries only.
   *
   * @return the detected capability or a safe failed-state diagnostic when inspection cannot complete
   */
  public VectorCapability inspect() {
    if (activeCapability != null) {
      return activeCapability;
    }
    VectorCapability capability = queryCapability();
    if (capability.getState() == VectorCapabilityState.ACTIVE) {
      // Only the ACTIVE state is remembered. Activation moves in one direction, so a cached ACTIVE
      // cannot go stale, whereas caching AVAILABLE or UNAVAILABLE would keep reporting the feature
      // as off after an administrator turns it on. Every other state is re-inspected.
      activeCapability = capability;
    }
    return capability;
  }

  private VectorCapability queryCapability() {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(CAPABILITY_SQL)) {
      statement.setString(1, VECTOR_EXTENSION);
      statement.setString(2, VECTOR_EXTENSION);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return new VectorCapability(VectorCapabilityState.FAILED,
              "the pgvector capability could not be inspected; see the log for the reason.");
        }
        if (resultSet.getBoolean("installed")) {
          return new VectorCapability(VectorCapabilityState.ACTIVE,
              "the extension is installed in this database.");
        }
        if (resultSet.getBoolean("available")) {
          return new VectorCapability(VectorCapabilityState.AVAILABLE,
              "the PostgreSQL server provides pgvector, but this database does not have the extension yet. \nThe next update.database creates it.");
        }
        return new VectorCapability(VectorCapabilityState.UNAVAILABLE,
            "the PostgreSQL server does not provide pgvector, so no update can install it here. \nInstall the pgvector package on the server first.");
      }
    } catch (Exception exception) {
      // Logged rather than swallowed: this method exists to diagnose, and the one thing an
      // administrator cannot act on is a diagnosis that hides why it failed.
      log.error("Could not inspect the PostgreSQL pgvector capability.", exception);
      return new VectorCapability(VectorCapabilityState.FAILED,
          "the pgvector capability could not be inspected; see the log for the reason.");
    }
  }
}
