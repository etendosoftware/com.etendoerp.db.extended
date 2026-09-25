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

import java.time.Duration;
import java.util.Collections;

import org.openbravo.dal.service.OBDal;
import org.openbravo.scheduling.ProcessBundle;
import org.openbravo.scheduling.ProcessLogger;
import org.openbravo.service.db.DalBaseProcess;
import org.openbravo.service.db.DalConnectionProvider;

/**
 * Scheduled Classic process that drains the generic vector outbox.
 *
 * <p>The process is intentionally independent from any indexed entity. It recovers events left in
 * {@code PROCESSING} by an interrupted execution, then delivers a bounded batch of pending events.
 * Failed events are not retried automatically: an administrator must requeue them after correcting
 * the underlying provider or source configuration.</p>
 */
public class ProcessVectorOutbox extends DalBaseProcess {
  static final int DEFAULT_BATCH_SIZE = 100;
  static final Duration STALE_PROCESSING_AGE = Duration.ofMinutes(15);
  /** How long a DONE or SUPERSEDED event is kept before the run removes it. */
  static final Duration TERMINAL_RETENTION = Duration.ofDays(30);
  /** Upper bound of events removed per run, so the delete never turns into a long lock. */
  static final int PURGE_BATCH_SIZE = 1000;

  @Override
  protected void doExecute(ProcessBundle bundle) throws Exception {
    ProcessLogger logger = bundle.getLogger();
    VectorOutboxService outbox = createOutboxService();

    int retired = outbox.exhaustStaleProcessing(STALE_PROCESSING_AGE, DEFAULT_BATCH_SIZE);
    int recovered = outbox.requeueStaleProcessing(STALE_PROCESSING_AGE, DEFAULT_BATCH_SIZE);
    int processed = outbox.processPending(DEFAULT_BATCH_SIZE);
    int purged = outbox.purgeTerminal(TERMINAL_RETENTION, PURGE_BATCH_SIZE);
    OBDal.getInstance().commitAndClose();

    logger.logln("Vector outbox completed. Recovered stale events=" + recovered
        + ", retired events=" + retired + ", processed events=" + processed
        + ", purged events=" + purged
        + ", batch size=" + DEFAULT_BATCH_SIZE + ".");
  }

  VectorOutboxService createOutboxService() {
    DalConnectionProvider connectionProvider = new DalConnectionProvider(false);
    VectorStore store = new VectorStoreService(connectionProvider);
    VectorOutboxConsumer consumer = new DictionaryVectorOutboxConsumer(connectionProvider, store);
    return new VectorOutboxService(connectionProvider, Collections.singletonList(consumer),
        new VectorOutboxService.TransactionBoundary() {
          @Override public void commit() {
            OBDal.getInstance().commitAndClose();
          }

          @Override public void rollback() {
            OBDal.getInstance().rollbackAndClose();
          }
        });
  }
}
