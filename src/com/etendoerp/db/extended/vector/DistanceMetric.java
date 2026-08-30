package com.etendoerp.db.extended.vector;

/** Whitelist of PostgreSQL distance operators. */
public enum DistanceMetric {
  COSINE("<=>", "vector_cosine_ops"), L2("<->", "vector_l2_ops"), INNER_PRODUCT("<#>", "vector_ip_ops");
  private final String operator;
  private final String operatorClass;
  DistanceMetric(String operator, String operatorClass) { this.operator = operator; this.operatorClass = operatorClass; }
  public String getOperator() { return operator; }
  public String getOperatorClass() { return operatorClass; }
}
