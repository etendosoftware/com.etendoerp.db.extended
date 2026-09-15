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

import java.util.Collection;

/** Deterministically resolves at most one optional extension per namespace. */
public final class VectorExtensionResolver {
  private final Collection<VectorExtension> extensions;
  public VectorExtensionResolver(Collection<VectorExtension> extensions) { this.extensions = extensions; }
  public VectorExtension resolve(String namespace) {
    VectorExtension match = null;
    for (VectorExtension extension : extensions) if (namespace.equals(extension.namespace())) {
      if (match != null) throw new VectorException(VectorErrorCode.VECTOR_EXTENSION_CONFLICT, "Multiple vector extensions claim namespace " + namespace + ".");
      match = extension;
    }
    return match;
  }
}
