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

  /**
   * Number of events the dispatcher may hand to a single {@link #prepare(java.util.List)} call.
   *
   * <p>It bounds the delivery chunk, which is also the transaction the dispatcher holds, so it
   * should match whatever the consumer can resolve in one remote round trip.</p>
   *
   * @param event
   *     any event of the group about to be delivered, so the consumer can size the chunk from its
   *     own per-source configuration
   * @return the number of events the dispatcher may deliver in one chunk
   */
  default int batchSize(VectorOutboxEvent event) {
    return 1;
  }

  /**
   * Resolves in one go everything the following {@link #consume(VectorOutboxEvent)} calls will
   * need, so an expensive remote call is paid once per chunk instead of once per event.
   *
   * <p>Implementing it is optional: the default does nothing and each event resolves itself. A failure
   * here fails the whole chunk, which is correct when the shared call is what failed.</p>
   *
   * <p>Failures are reported as {@link VectorException}: a consumer that cannot resolve its chunk
   * has failed at something this module defines, and the dispatcher treats it as such.</p>
   *
   * @param events
   *     the events about to be delivered as one chunk
   */
  default void prepare(java.util.List<VectorOutboxEvent> events) {
    // Nothing to resolve ahead of time.
  }
}
