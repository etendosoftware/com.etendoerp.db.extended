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

import java.util.Collections;

import org.openbravo.dal.service.OBDal;
import org.openbravo.scheduling.ProcessBundle;
import org.openbravo.scheduling.ProcessLogger;
import org.openbravo.service.db.DalBaseProcess;
import org.openbravo.service.db.DalConnectionProvider;

/**
 * Puts failed indexing events back in the queue.
 *
 * <p>Failed events are never retried on their own: whatever broke, broke for a reason, and
 * redelivering it on a timer would spend provider calls reproducing the same failure. Recovery is
 * therefore an explicit decision, taken once the provider or source configuration is corrected.</p>
 *
 * <p>It also resets the attempt counter, because this is that deliberate decision and the event is
 * entitled to a full budget again. Core follows the same shape in
 * {@code ImportReprocessErrorEntries}, which flips its own failed entries back in bulk.</p>
 */
public class ProcessVectorRequeueFailed extends DalBaseProcess {

  /** Upper bound per run, so a large backlog of failures is recovered gradually. */
  static final int BATCH_SIZE = 1000;

  @Override
  protected void doExecute(ProcessBundle bundle) throws Exception {
    ProcessLogger logger = bundle.getLogger();
    DalConnectionProvider connectionProvider = new DalConnectionProvider(false);
    VectorOutboxService outbox = new VectorOutboxService(connectionProvider, Collections.emptyList());

    int requeued = outbox.requeueFailed(BATCH_SIZE);
    OBDal.getInstance().commitAndClose();

    logger.logln("Vector failed events requeued=" + requeued + ", batch size=" + BATCH_SIZE + ".");
  }
}
