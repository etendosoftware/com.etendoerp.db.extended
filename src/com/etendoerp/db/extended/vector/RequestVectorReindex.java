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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.dal.service.OBDal;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.service.db.DalConnectionProvider;

import com.etendoerp.db.extended.data.VectorSource;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Candidate;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Verdict;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Asks for the records a source already held to be indexed.
 *
 * <p>The triggers only capture what changes from the moment they are installed, so a source
 * configured over a table that already has rows indexes nothing until those rows are walked. This
 * is how that walk is asked for.</p>
 *
 * <p>It writes the request and stops there. Walking the table is the scheduled process's work: a
 * source over a document table can be millions of rows, and doing it here would hold the session
 * for as long as it took and fill the queue faster than delivery drains it. What the
 * administrator gets back instead is roughly how many records they just asked for.</p>
 *
 * <p>A source that could not be delivered is refused rather than queued. Enqueueing a whole table
 * for a source with no content column produces one failure per row, and the queue is then full of
 * events that can only fail. The verdict is the same one activation reads, so the two windows can
 * never disagree about what ready means.</p>
 */
public class RequestVectorReindex extends Action {

  private static final Logger log = LogManager.getLogger();

  @Override
  protected Class<?> getInputClass() {
    return VectorSource.class;
  }

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    ActionResult result = new ActionResult();
    List<String> outcomes = new ArrayList<>();
    boolean allAccepted = true;
    try {
      ConnectionProvider connectionProvider = new DalConnectionProvider(false);

      // Read while the records are still attached: the request commits, and an entity read after
      // that is detached.
      List<Candidate> candidates = new ArrayList<>();
      for (VectorSource source : getInputContents(VectorSource.class)) {
        candidates.add(VectorSourceReadiness.candidate(connectionProvider, source));
      }

      VectorReindexService reindex = new VectorReindexService(connectionProvider,
          new VectorOutboxService.TransactionBoundary() {
            @Override
            public void commit() {
              OBDal.getInstance().commitAndClose();
            }

            @Override
            public void rollback() {
              OBDal.getInstance().rollbackAndClose();
            }
          });

      for (Candidate candidate : candidates) {
        Verdict verdict = VectorSourceReadiness.verdict(candidate,
            VectorSourceReadiness.collection(connectionProvider, candidate.namespace));
        if (!verdict.isUsable()) {
          allAccepted = false;
          outcomes.add(candidate.name + ": " + OBMessageUtils.messageBD(verdict.getMessageKey()));
          continue;
        }
        VectorReindexService.Outcome outcome =
            reindex.requestReindex(candidate.id, confirmRestart(parameters));
        allAccepted &= outcome.getResult().isAccepted();
        outcomes.add(candidate.name + ": " + describe(outcome));
      }
      OBDal.getInstance().commitAndClose();

      result.setType(allAccepted ? Result.Type.SUCCESS : Result.Type.WARNING);
      result.setMessage(String.join("\n", outcomes));
    } catch (Exception e) {
      log.error("Vector reindex request failed", e);
      OBDal.getInstance().rollbackAndClose();
      result.setType(Result.Type.ERROR);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  /**
   * Whether the administrator has already been told what restarting an existing walk would cost.
   *
   * <p>Absent means no. A source that was never walked is requested either way, so the parameter
   * only ever gates the destructive case.</p>
   */
  private boolean confirmRestart(JSONObject parameters) {
    return parameters != null && parameters.optBoolean("Confirm_Restart", false);
  }

  /** Says what happened, with the numbers each answer needs to be acted on. */
  private String describe(VectorReindexService.Outcome outcome) {
    String key = outcome.getResult().getMessageKey();
    switch (outcome.getResult()) {
      case REQUESTED:
      case RESTARTED:
        return OBMessageUtils.getI18NMessage(key,
            new String[] { String.valueOf(outcome.getEstimate()) });
      case NEEDS_CONFIRMATION:
        return OBMessageUtils.getI18NMessage(key,
            new String[] { String.valueOf(outcome.getAlreadyEnqueued()),
                String.valueOf(outcome.getEstimate()) });
      default:
        return OBMessageUtils.messageBD(key);
    }
  }
}
