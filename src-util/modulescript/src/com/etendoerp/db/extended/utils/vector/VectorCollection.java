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
package com.etendoerp.db.extended.utils.vector;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Immutable generic vector collection definition.
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.VectorCollection} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 */
public final class VectorCollection {
  private static final Pattern NAMESPACE = Pattern.compile("^[A-Za-z][A-Za-z0-9_.-]{0,127}$");
  private final String namespace; private final int dimensions; private final DistanceMetric metric;
  private final boolean clientScoped; private final boolean organizationScoped;
  public VectorCollection(String namespace, int dimensions, DistanceMetric metric, boolean clientScoped, boolean organizationScoped) {
    if (namespace == null || !NAMESPACE.matcher(namespace).matches()) throw new IllegalArgumentException("Invalid vector namespace");
    if (dimensions < 1 || dimensions > 2000) throw new IllegalArgumentException("Vector dimensions must be between 1 and 2000");
    this.namespace = namespace; this.dimensions = dimensions; this.metric = Objects.requireNonNull(metric, "metric");
    this.clientScoped = clientScoped; this.organizationScoped = organizationScoped;
  }
  public String getNamespace() { return namespace; } public int getDimensions() { return dimensions; }
  public DistanceMetric getMetric() { return metric; } public boolean isClientScoped() { return clientScoped; }
  public boolean isOrganizationScoped() { return organizationScoped; }
}
