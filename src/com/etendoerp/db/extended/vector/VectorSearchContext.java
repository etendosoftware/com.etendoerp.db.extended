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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.openbravo.dal.core.OBContext;

/**
 * Tenant scope for semantic search. It is intentionally built only from the active Etendo context.
 */
final class VectorSearchContext {
  private final String clientId;
  private final String organizationId;
  private final List<String> organizationIds;

  private VectorSearchContext(String clientId, String organizationId, List<String> organizationIds) {
    this.clientId = clientId;
    this.organizationId = organizationId;
    this.organizationIds = organizationIds;
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
    LinkedHashSet<String> readableOrganizations = new LinkedHashSet<>();
    String[] readableOrganizationIds = context.getReadableOrganizations();
    if (readableOrganizationIds != null) {
      for (String organizationId : readableOrganizationIds) {
        if (!isBlank(organizationId)) readableOrganizations.add(organizationId);
      }
    }
    readableOrganizations.add(context.getCurrentOrganization().getId());
    return new VectorSearchContext(context.getCurrentClient().getId(),
        context.getCurrentOrganization().getId(),
        new ArrayList<>(readableOrganizations));
  }

  String getClientId() {
    return clientId;
  }

  String getOrganizationId() {
    return organizationId;
  }

  List<String> getOrganizationIds() {
    return new ArrayList<>(organizationIds);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
