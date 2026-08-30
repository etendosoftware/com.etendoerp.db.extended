package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import org.openbravo.database.ConnectionProvider;

/** Explicit administrator-only lifecycle. It is never invoked by DBSM, startup, or module scripts. */
public class VectorActivationService {
  private final ConnectionProvider cp; private final VectorCapabilityService capabilityService;
  public VectorActivationService(ConnectionProvider cp) { this.cp = cp; capabilityService = new VectorCapabilityService(cp); }
  public VectorCapability activate() {
    VectorCapability capability = capabilityService.inspect();
    if (capability.getState() == VectorCapabilityState.UNAVAILABLE) throw disabled(capability);
    try {
      execute("CREATE TABLE IF NOT EXISTS etarc_vector_activation (id boolean primary key default true, state varchar(16) not null, diagnostic text, updated_at timestamptz not null default now())");
      execute("CREATE EXTENSION IF NOT EXISTS vector");
      execute("CREATE TABLE IF NOT EXISTS etarc_vector_collection (id bigserial primary key, namespace varchar(128) not null unique, dimensions integer not null, metric varchar(32) not null, client_scoped boolean not null, organization_scoped boolean not null, active boolean not null default true, index_status varchar(16) not null default 'NOT_CREATED')");
      execute("CREATE TABLE IF NOT EXISTS etarc_vector_record (namespace varchar(128) not null references etarc_vector_collection(namespace) on delete cascade, external_key varchar(255) not null, client_id varchar(32) not null default '', organization_id varchar(32) not null default '', embedding vector not null, metadata jsonb not null default '{}'::jsonb, created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key(namespace, external_key, client_id, organization_id))");
      execute("INSERT INTO etarc_vector_activation (id, state, diagnostic) VALUES (true, 'ACTIVE', null) ON CONFLICT (id) DO UPDATE SET state = 'ACTIVE', diagnostic = null, updated_at = now()");
      return new VectorCapability(VectorCapabilityState.ACTIVE, "The pgvector extension and generic storage are active.");
    } catch (Exception e) {
      persistFailure();
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, "pgvector activation failed; verify extension permissions and retry.", e);
    }
  }
  private void persistFailure() { try { execute("CREATE TABLE IF NOT EXISTS etarc_vector_activation (id boolean primary key default true, state varchar(16) not null, diagnostic text, updated_at timestamptz not null default now())"); execute("INSERT INTO etarc_vector_activation (id, state, diagnostic) VALUES (true, 'FAILED', 'Activation failed; verify extension permissions and retry.') ON CONFLICT (id) DO UPDATE SET state = 'FAILED', diagnostic = EXCLUDED.diagnostic, updated_at = now()"); } catch (Exception ignored) { } }
  private void execute(String sql) throws Exception { try (PreparedStatement ps = cp.getPreparedStatement(sql)) { ps.executeUpdate(); } }
  static VectorException disabled(VectorCapability capability) { return new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, capability.getDiagnostic()); }

  /** Read-only persisted activation check used by all operational entry points. */
  static boolean isActivated(ConnectionProvider cp) {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT state = 'ACTIVE' FROM etarc_vector_activation WHERE id = true");
        java.sql.ResultSet rs = ps.executeQuery()) {
      return rs.next() && rs.getBoolean(1);
    } catch (Exception ignored) {
      return false;
    }
  }
}
