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
