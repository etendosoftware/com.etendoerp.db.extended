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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/** Explicit administrator-only lifecycle. It is never invoked by DBSM, startup, or module scripts. */
public class VectorActivationService {
  private static final Logger log = LogManager.getLogger();

  private final ConnectionProvider cp; private final VectorCapabilityService capabilityService;
  public VectorActivationService(ConnectionProvider cp) { this.cp = cp; capabilityService = new VectorCapabilityService(cp); }
  public VectorCapability activate() {
    VectorCapability capability = capabilityService.inspect();
    if (capability.getState() == VectorCapabilityState.UNAVAILABLE) throw disabled(capability);
    try {
      VectorRuntimeSchema.ensure(cp);
      execute("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_activation (id boolean primary key default true, state varchar(16) not null, diagnostic text, updated_at timestamptz not null default now())");
      execute("CREATE EXTENSION IF NOT EXISTS vector");
      execute("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_collection (id bigserial primary key, namespace varchar(128) not null unique, dimensions integer not null, metric varchar(32) not null, client_scoped boolean not null, organization_scoped boolean not null, active boolean not null default true, index_status varchar(16) not null default 'NOT_CREATED')");
      execute("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_record (namespace varchar(128) not null references etarc_vector.etarc_vector_collection(namespace) on delete cascade, external_key varchar(255) not null, client_id varchar(32) not null default '', organization_id varchar(32) not null default '', embedding vector not null, metadata jsonb not null default '{}'::jsonb, created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key(namespace, external_key, client_id, organization_id))");
      execute("INSERT INTO etarc_vector.etarc_vector_activation (id, state, diagnostic) VALUES (true, 'ACTIVE', null) ON CONFLICT (id) DO UPDATE SET state = 'ACTIVE', diagnostic = null, updated_at = now()");
      return new VectorCapability(VectorCapabilityState.ACTIVE, "The pgvector extension and generic storage are active.");
    } catch (Exception e) {
      persistFailure();
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, "pgvector activation failed; verify extension permissions and retry.", e);
    }
  }
  private void persistFailure() {
    try {
      VectorRuntimeSchema.ensure(cp);
      execute("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_activation (id boolean primary"
          + " key default true, state varchar(16) not null, diagnostic text, updated_at timestamptz"
          + " not null default now())");
      execute("INSERT INTO etarc_vector.etarc_vector_activation (id, state, diagnostic)"
          + " VALUES (true, 'FAILED', 'Activation failed; verify extension permissions and retry.')"
          + " ON CONFLICT (id) DO UPDATE SET state = 'FAILED', diagnostic = EXCLUDED.diagnostic,"
          + " updated_at = now()");
    } catch (Exception ignored) {
      // Recording the failure is best effort: the caller is already throwing the failure itself,
      // and a database that cannot take this row is usually the very reason activation failed.
    }
  }
  private void execute(String sql) throws Exception { try (PreparedStatement ps = cp.getPreparedStatement(sql)) { ps.executeUpdate(); } }
  static VectorException disabled(VectorCapability capability) { return new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, capability.getDiagnostic()); }

  /**
   * Read-only persisted activation check used by all operational entry points.
   *
   * <p>Asked in two steps, and never in one. Before the first update provisions anything the
   * activation table does not exist, and querying a missing relation does not merely fail: it
   * aborts the whole JDBC transaction, so every later statement is refused with "current
   * transaction is aborted" -- including the ones that read messages, which is how a caller ends up
   * reporting untranslated keys and a capability it could not inspect. {@code to_regclass} answers
   * the same question without raising.</p>
   */
  static boolean isActivated(ConnectionProvider cp) {
    try {
      if (!storageExists(cp)) {
        return false;
      }
      try (PreparedStatement ps = cp.getPreparedStatement(
          "SELECT state = 'ACTIVE' FROM etarc_vector.etarc_vector_activation WHERE id = true");
          java.sql.ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    } catch (Exception e) {
      log.debug("Could not read the vector activation state.", e);
      return false;
    }
  }

  private static boolean storageExists(ConnectionProvider cp) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT to_regclass('etarc_vector.etarc_vector_activation') IS NOT NULL");
        java.sql.ResultSet rs = ps.executeQuery()) {
      return rs.next() && rs.getBoolean(1);
    }
  }
}
