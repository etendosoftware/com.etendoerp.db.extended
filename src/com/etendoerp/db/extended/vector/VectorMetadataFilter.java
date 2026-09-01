package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A parameterized metadata predicate. Configuration compilers create this type; callers never
 * supply SQL fragments. Its composition API is intentionally reusable by a future RSQL compiler.
 */
final class VectorMetadataFilter {
  private final String clause;
  private final List<String> parameters;

  private VectorMetadataFilter(String clause, List<String> parameters) {
    this.clause = clause;
    this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
  }

  static VectorMetadataFilter jsonContains(String json) {
    String value = json == null ? "{}" : json;
    return new VectorMetadataFilter("(?::jsonb = '{}'::jsonb OR metadata @> ?::jsonb)",
        java.util.Arrays.asList(value, value));
  }

  static VectorMetadataFilter predicate(String clause, List<String> parameters) {
    return new VectorMetadataFilter(clause, parameters);
  }

  VectorMetadataFilter and(VectorMetadataFilter other) {
    List<String> combined = new ArrayList<>(parameters);
    combined.addAll(other.parameters);
    return new VectorMetadataFilter("(" + clause + ") AND (" + other.clause + ")", combined);
  }

  String getClause() { return clause; }

  int bind(PreparedStatement statement, int index) throws Exception {
    for (String parameter : parameters) statement.setString(index++, parameter);
    return index;
  }
}
