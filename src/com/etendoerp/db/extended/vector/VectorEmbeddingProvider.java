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

import java.util.List;

/** Provider-neutral conversion of indexable text into a vector. */
public interface VectorEmbeddingProvider {
  /**
   * Embeds several texts in a single provider request.
   *
   * @param texts
   *     inputs to embed, at most {@link #batchSize()} of them
   * @return one embedding per input, in the same order
   */
  List<double[]> embed(List<String> texts);

  /** Maximum number of inputs the provider accepts in one request. */
  int batchSize();

  int dimensions();

  /** Embeds a single text. Kept for callers that have nothing to batch. */
  default double[] embed(String text) {
    return embed(List.of(text)).get(0);
  }
}
