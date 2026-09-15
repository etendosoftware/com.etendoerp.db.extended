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

import java.util.Arrays;
import java.util.Objects;

/** Immutable, entity-agnostic vector record. */
public final class VectorRecord {
  private final String namespace, key, clientId, organizationId, metadata;
  private final double[] vector;
  public VectorRecord(String namespace, String key, double[] vector, String metadata, String clientId, String organizationId) {
    this.namespace = Objects.requireNonNull(namespace, "namespace"); this.key = Objects.requireNonNull(key, "key");
    if (key.length() == 0 || key.length() > 255) throw new IllegalArgumentException("Invalid vector external key");
    this.vector = Arrays.copyOf(Objects.requireNonNull(vector, "vector"), vector.length);
    if (vector.length == 0) throw new IllegalArgumentException("Vector cannot be empty");
    this.metadata = metadata == null ? "{}" : metadata; this.clientId = clientId; this.organizationId = organizationId;
  }
  public String getNamespace() { return namespace; } public String getKey() { return key; }
  public double[] getVector() { return Arrays.copyOf(vector, vector.length); } public String getMetadata() { return metadata; }
  public String getClientId() { return clientId; } public String getOrganizationId() { return organizationId; }
}
