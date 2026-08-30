/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing rights
 * and limitations under the License.
 * All portions are Copyright © 2021–2025 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */

package com.etendoerp.db.extended.vector;

import java.util.Objects;

/**
 * Immutable result of a read-only pgvector capability inspection.
 */
public final class VectorCapability {
  private final VectorCapabilityState state;
  private final String diagnostic;

  VectorCapability(VectorCapabilityState state, String diagnostic) {
    this.state = Objects.requireNonNull(state, "state");
    this.diagnostic = Objects.requireNonNull(diagnostic, "diagnostic");
  }

  public VectorCapabilityState getState() {
    return state;
  }

  /**
   * Returns a safe diagnostic suitable for administrative logs or API responses.
   */
  public String getDiagnostic() {
    return diagnostic;
  }
}
