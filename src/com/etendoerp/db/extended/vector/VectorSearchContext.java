package com.etendoerp.db.extended.vector;

import org.openbravo.dal.core.OBContext;

/**
 * Tenant scope for semantic search. It is intentionally built only from the active Etendo context.
 */
final class VectorSearchContext {
  private final String clientId;
  private final String organizationId;

  private VectorSearchContext(String clientId, String organizationId) {
    this.clientId = clientId;
    this.organizationId = organizationId;
  }

  static VectorSearchContext current() {
    OBContext context = OBContext.getOBContext();
    if (context == null || context.getCurrentClient() == null
        || isBlank(context.getCurrentClient().getId())) {
      throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
          "An active Etendo client context is required for semantic search.");
    }
    if (context.getCurrentOrganization() == null
        || isBlank(context.getCurrentOrganization().getId())) {
      throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
          "An active Etendo organization context is required for semantic search.");
    }
    return new VectorSearchContext(context.getCurrentClient().getId(),
        context.getCurrentOrganization().getId());
  }

  String getClientId() {
    return clientId;
  }

  String getOrganizationId() {
    return organizationId;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
