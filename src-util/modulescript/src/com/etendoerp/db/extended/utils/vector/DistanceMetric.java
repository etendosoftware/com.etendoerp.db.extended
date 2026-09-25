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

/**
 * Whitelist of PostgreSQL distance operators.
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.DistanceMetric} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 */
public enum DistanceMetric {
  COSINE("<=>", "vector_cosine_ops"), L2("<->", "vector_l2_ops"), INNER_PRODUCT("<#>", "vector_ip_ops");
  private final String operator;
  private final String operatorClass;
  DistanceMetric(String operator, String operatorClass) { this.operator = operator; this.operatorClass = operatorClass; }
  public String getOperator() { return operator; }
  public String getOperatorClass() { return operatorClass; }
}
