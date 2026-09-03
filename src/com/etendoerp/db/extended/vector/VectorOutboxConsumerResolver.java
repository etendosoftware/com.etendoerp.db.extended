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

import java.util.Collection;

/** Deterministically resolves at most one outbox consumer for a namespace. */
public final class VectorOutboxConsumerResolver {
  private final Collection<VectorOutboxConsumer> consumers;

  public VectorOutboxConsumerResolver(Collection<VectorOutboxConsumer> consumers) {
    this.consumers = consumers;
  }

  public VectorOutboxConsumer resolve(String namespace) {
    VectorOutboxConsumer match = null;
    for (VectorOutboxConsumer consumer : consumers) {
      if (consumer.supports(namespace)) {
        if (match != null) {
          throw new VectorException(VectorErrorCode.VECTOR_EXTENSION_CONFLICT,
              "Multiple vector outbox consumers claim namespace " + namespace + ".");
        }
        match = consumer;
      }
    }
    return match;
  }
}
