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

import org.openbravo.dal.service.OBDal;
import org.openbravo.scheduling.ProcessBundle;
import org.openbravo.scheduling.ProcessLogger;
import org.openbravo.service.db.DalBaseProcess;
import org.openbravo.service.db.DalConnectionProvider;

/**
 * Scheduled Classic process that enqueues the records a source held before it was instrumented.
 *
 * <p>It is deliberately separate from {@link ProcessVectorOutbox}. The two have opposite shapes:
 * this one writes in bulk against the database, the other spends its time waiting on an external
 * provider one record at a time. Sharing a schedule would starve the consumer while a backfill
 * runs, and the two cannot sensibly run at the same frequency.</p>
 *
 * <p>What keeps them from fighting is the backlog limit: the reindex stops enqueueing once the
 * source has enough events waiting, and resumes on a later run from the cursor it stored.</p>
 */
public class ProcessVectorReindex extends DalBaseProcess {

  /** Records enqueued per statement, which is also the transaction held while doing it. */
  static final int CHUNK_SIZE = 1000;
  /** Upper bound of chunks per run, so one run cannot monopolise the scheduler. */
  static final int MAX_CHUNKS = 20;
  /** Stop enqueueing when the source already has this many events waiting to be delivered. */
  static final int BACKLOG_LIMIT = 10000;

  @Override
  protected void doExecute(ProcessBundle bundle) throws Exception {
    ProcessLogger logger = bundle.getLogger();
    DalConnectionProvider connectionProvider = new DalConnectionProvider(false);
    VectorReindexService reindex = new VectorReindexService(connectionProvider,
        new VectorOutboxService.TransactionBoundary() {
          @Override public void commit() {
            OBDal.getInstance().commitAndClose();
          }

          @Override public void rollback() {
            OBDal.getInstance().rollbackAndClose();
          }
        });

    int enqueued = reindex.process(CHUNK_SIZE, MAX_CHUNKS, BACKLOG_LIMIT);
    OBDal.getInstance().commitAndClose();

    logger.logln("Vector reindex completed. Enqueued records=" + enqueued
        + ", chunk size=" + CHUNK_SIZE + ", backlog limit=" + BACKLOG_LIMIT + ".");
  }
}
