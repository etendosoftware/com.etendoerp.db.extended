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

import java.util.ArrayList; import java.util.Arrays; import java.util.Collection; import java.util.Collections; import java.util.List; import java.util.Objects;

/** Immutable exact-search request. Metadata is a JSON object, never a SQL fragment. */
public final class VectorQuery {
  private final String namespace, metadata; private final VectorMetadataFilter metadataFilter; private final double[] vector; private final int topK; private final DistanceMetric metric;
  private final String clientId; private final List<String> organizationIds;

  /** Restricts the search to a single organization. */
  public VectorQuery(String namespace, double[] vector, int topK, DistanceMetric metric, String metadata, String clientId, String organizationId) {
    this(namespace, vector, topK, metric, metadata, clientId,
        organizationId == null ? Collections.emptyList() : Collections.singletonList(organizationId));
  }

  /**
   * Restricts the search to any of the supplied organizations.
   *
   * <p>Asking for all of them at once returns the same matches as one query per organization: the
   * union of the per-organization top results always contains the global ones. It just stops
   * issuing a round trip per readable organization, which on a deep tree dominated the cost of a
   * single search.</p>
   */
  public VectorQuery(String namespace, double[] vector, int topK, DistanceMetric metric, String metadata, String clientId, Collection<String> organizationIds) {
    this(namespace, vector, topK, metric, metadata, VectorMetadataFilter.jsonContains(metadata == null ? "{}" : metadata), clientId, organizationIds);
  }

  VectorQuery(String namespace, double[] vector, int topK, DistanceMetric metric, String metadata, VectorMetadataFilter metadataFilter, String clientId, Collection<String> organizationIds) {
    this.namespace = Objects.requireNonNull(namespace, "namespace"); this.vector = Arrays.copyOf(Objects.requireNonNull(vector, "vector"), vector.length);
    if (vector.length == 0 || topK < 1 || topK > 1000) throw new IllegalArgumentException("Invalid vector query");
    this.topK = topK; this.metric = Objects.requireNonNull(metric, "metric"); this.metadata = metadata == null ? "{}" : metadata; this.metadataFilter = Objects.requireNonNull(metadataFilter, "metadataFilter");
    this.clientId = clientId;
    this.organizationIds = organizationIds == null ? Collections.emptyList()
        : Collections.unmodifiableList(new ArrayList<>(organizationIds));
  }
  public String getNamespace() { return namespace; } public double[] getVector() { return Arrays.copyOf(vector, vector.length); }
  public int getTopK() { return topK; } public DistanceMetric getMetric() { return metric; } public String getMetadata() { return metadata; }
  VectorMetadataFilter getMetadataFilter() { return metadataFilter; }
  public String getClientId() { return clientId; }

  /** Organizations the search is restricted to, empty when it is not restricted. */
  public List<String> getOrganizationIds() { return organizationIds; }

  /** @deprecated use {@link #getOrganizationIds()}; returns the first one, if any. */
  @Deprecated
  public String getOrganizationId() { return organizationIds.isEmpty() ? null : organizationIds.get(0); }
}
