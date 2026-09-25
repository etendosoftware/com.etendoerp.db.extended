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
