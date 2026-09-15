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

/** Physical source plus its target-owned, already validated metadata predicate. */
final class VectorSearchTarget {
  private final String key;
  private final VectorSearchSource source;
  private final VectorMetadataFilter filter;

  VectorSearchTarget(String key, VectorSearchSource source, VectorMetadataFilter filter) {
    this.key = key;
    this.source = source;
    this.filter = filter;
  }

  String getKey() { return key; }
  VectorSearchSource getSource() { return source; }
  VectorMetadataFilter getFilter() { return filter; }
}
