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
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

/**
 * Namespace-owned extension point for turning a generic source event into an indexed vector.
 *
 * <p>Consumers own fetching the source record and embedding generation. The DB Extended module
 * owns only event delivery and its durable status lifecycle.</p>
 */
public interface VectorOutboxConsumer {
  String namespace();

  default boolean supports(String candidateNamespace) { return namespace().equals(candidateNamespace); }

  void consume(VectorOutboxEvent event) throws Exception;
}
