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
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Collection;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Verdict;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Reports whether the selected search sources are ready to be indexed, and what is stopping the
 * ones that are not.
 *
 * <p>It changes nothing. Everything a source needs -- the extension, the runtime storage, its
 * collection and its change capture -- is created by the next {@code update.database}, which is
 * the only moment at which that DDL is free: the update that performs it is also the one that
 * accepts the structure it leaves behind. A button doing it while the application runs would have
 * to accept that structure on the administrator's behalf, and with it whatever else had been
 * changed in the database and not yet exported.</p>
 *
 * <p>So configuring a source is two steps: save it, then run update.database. This is how an
 * administrator finds out, before running it, whether the configuration will actually produce
 * anything.</p>
 *
 * <p>The checks exist because a source can be broken in ways that only surface much later. A
 * source with no content column is accepted by the dictionary but fails on every delivery with a
 * no content columns error, once per event, until the retry limit gives up; a collection created
 * before the provider changed holds vectors of the wrong size, and nothing would reject the new
 * ones until they reach the database. This is where an administrator asks "is this source ready?",
 * rather than finding out from a pile of FAILED outbox rows nobody is watching.</p>
 *
 * <p>Drift is reported and never repaired, here or anywhere else. Making a collection match again
 * means dropping it, and that deletes every vector it holds; whether re-embedding the whole table
 * is worth it is the administrator's call.</p>
 */
public class CheckVectorSource extends Action {

  private static final Logger log = LogManager.getLogger();

  @Override
  protected Class<?> getInputClass() {
    return VectorSource.class;
  }

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    ActionResult result = new ActionResult();
    try {
      ConnectionProvider connectionProvider = new DalConnectionProvider(false);
      List<Candidate> candidates = new ArrayList<>();
      for (VectorSource source : getInputContents(VectorSource.class)) {
        candidates.add(VectorSourceReadiness.candidate(connectionProvider, source));
      }

      Report report = check(candidates, connectionProvider);

      // A source that cannot be indexed as configured is not a success, however healthy the
      // database is: the point of asking is to hear that.
      result.setType(report.allReady() ? Result.Type.SUCCESS : Result.Type.WARNING);
      result.setMessage(render(report));
    } catch (Exception e) {
      log.error("Could not check the vector search sources", e);
      OBDal.getInstance().rollbackAndClose();
      result.setType(Result.Type.ERROR);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  /**
   * Judges each source against the database as it stands.
   *
   * <p>Separate from {@link #action} so what it reads can be read: no message resolution, no DAL
   * session, and not one statement that writes.</p>
   *
   * @param candidates
   *     the sources to judge
   * @param connectionProvider
   *     connection the database is inspected with
   * @return what each source turned out to be
   * @throws Exception
   *     if the database cannot be inspected
   */
  Report check(List<Candidate> candidates, ConnectionProvider connectionProvider) throws Exception {
    // Before the first update provisions anything there is no collection table to ask, and every
    // source is simply waiting for that update. Asking anyway would fail with a missing relation,
    // which is a worse answer than the true one.
    boolean provisioned = VectorActivationService.isActivated(connectionProvider);
    VectorCapability capability = new VectorCapabilityService(connectionProvider).inspect();

    List<Line> lines = new ArrayList<>();
    for (Candidate candidate : candidates) {
      Collection collection = provisioned
          ? VectorSourceReadiness.collection(connectionProvider, candidate.namespace)
          : null;
      lines.add(new Line(candidate, collection, VectorSourceReadiness.verdict(candidate, collection)));
    }
    return new Report(capability,
        VectorActivationService.recordedFailure(connectionProvider), lines);
  }

  // --- turning the report into what the administrator reads -----------------------------------

  private String render(Report report) {
    List<String> outcomes = new ArrayList<>();
    outcomes.add(OBMessageUtils.messageBD("ETARC_VectorActivationState") + " "
        + report.capability.getState() + ". " + report.capability.getDiagnostic());
    if (report.failure != null) {
      outcomes.add(report.failure);
    }
    for (Line line : report.lines) {
      outcomes.add(line.candidate.name + ": " + render(line));
    }
    return String.join("\n", outcomes);
  }

  private String render(Line line) {
    String[] params = line.messageParameters();
    return params.length == 0 ? OBMessageUtils.messageBD(line.verdict.getMessageKey())
        : OBMessageUtils.getI18NMessage(line.verdict.getMessageKey(), params);
  }

  // --- reading what the run needs -------------------------------------------------------------

  // --- what the run carries -------------------------------------------------------------------

  /** What happened to one source. */
  static final class Line {
    final Candidate candidate;
    final Collection collection;
    final Verdict verdict;

    Line(Candidate candidate, Collection collection, Verdict verdict) {
      this.candidate = candidate;
      this.collection = collection;
      this.verdict = verdict;
    }

    /** Drift is the only verdict that has to say which two values disagree. */
    String[] messageParameters() {
      if (verdict == Verdict.DIMENSION_DRIFT) {
        return new String[] { String.valueOf(collection.dimensions),
            String.valueOf(candidate.dimensions) };
      }
      if (verdict == Verdict.METRIC_DRIFT) {
        return new String[] { collection.metric, candidate.metric };
      }
      return new String[0];
    }
  }

  /** What a whole check amounted to. */
  static final class Report {
    final VectorCapability capability;
    /** What the last update recorded when it could not provision, or {@code null}. */
    final String failure;
    final List<Line> lines;

    Report(VectorCapability capability, String failure, List<Line> lines) {
      this.capability = capability;
      this.failure = failure;
      this.lines = lines;
    }

    /**
     * Whether the check can say yes.
     *
     * <p>The capability counts: a database that could not even be inspected says nothing about its
     * sources, and reporting that as a success is how a broken environment passes for a working
     * one.</p>
     */
    boolean allReady() {
      return failure == null
          && capability.getState() != VectorCapabilityState.FAILED
          && lines.stream().allMatch(line -> line.verdict.isUsable());
    }
  }
}
