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
 * Turns vector indexing on for the selected search sources, and reports the ones that are not
 * ready to be turned on.
 *
 * <p>Nothing installs pgvector on its own: compiling the module, running update.database or
 * starting the application must never create an extension or a vector object. Activation is a
 * deliberate administrator action, and this is it.</p>
 *
 * <p>The button sits on the Search Source window because that is where a source is configured,
 * and it takes several records at a time so one source, a few or all of them can be activated in
 * a single step. Each run installs the extension and the runtime storage, once per database,
 * which is why the bootstrap runs even when a single source is selected. Then it looks at each
 * selected source and either creates its collection or explains what is stopping it.</p>
 *
 * <p>The checks exist because a source can be broken in ways that only surface much later. A
 * source with no content column is accepted by the dictionary but fails on every delivery with a
 * no content columns error, once per event, until the retry limit gives up; a collection created
 * before the provider changed holds vectors of the wrong size, and nothing would reject the new
 * ones until they reach the database. Activation is the moment an administrator asks "is this
 * source ready?", so that is where the answer belongs, rather than in a pile of FAILED outbox
 * rows nobody is watching.</p>
 *
 * <p>Drift is reported and never repaired. Making a collection match again means dropping it, and
 * that deletes every vector it holds; whether re-embedding the whole table is worth it is the
 * administrator's call, not this button's.</p>
 *
 * <p>Running it again is harmless: the storage is created only if absent and a source that is
 * already set up is reported as such and left alone. That also makes it the way to finish setting
 * up a source added after the database was activated.</p>
 */
public class ActivateVectorSource extends Action {

  private static final Logger log = LogManager.getLogger();

  /**
   * Where the run makes its work durable.
   *
   * <p>The same seam {@link VectorOutboxService} uses, and for the same reason: the order in which
   * a run commits is a guarantee it rests on, and it leaves no trace afterwards that a test could
   * read.</p>
   */
  @FunctionalInterface
  interface Checkpoint {
    /** Commits the work accumulated so far, making it visible to other sessions. */
    void commit();
  }

  @Override
  protected Class<?> getInputClass() {
    return VectorSource.class;
  }

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    ActionResult result = new ActionResult();
    try {
      ConnectionProvider connectionProvider = new DalConnectionProvider(false);

      // Read everything the run needs while the records are still attached. Activation commits and
      // closes the DAL session, and an entity read after that is detached.
      List<Candidate> candidates = new ArrayList<>();
      for (VectorSource source : getInputContents(VectorSource.class)) {
        candidates.add(VectorSourceReadiness.candidate(connectionProvider, source));
      }

      Report report = run(candidates, connectionProvider, new VectorStoreService(connectionProvider),
          () -> OBDal.getInstance().commitAndClose());

      // A run that left a source unusable is not a success, however well the bootstrap went.
      result.setType(report.allReady() ? Result.Type.SUCCESS : Result.Type.WARNING);
      result.setMessage(render(report));
    } catch (Exception e) {
      log.error("Vector source activation failed", e);
      OBDal.getInstance().rollbackAndClose();
      result.setType(Result.Type.ERROR);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  /**
   * Activates the database and every selected source, and reports what happened to each.
   *
   * <p>Separate from {@link #action} so the order it works in can be read: nothing here resolves a
   * message, touches the DAL session directly or needs a dictionary record, and the order of the
   * statements it issues is the behaviour worth checking.</p>
   */
  Report run(List<Candidate> candidates, ConnectionProvider connectionProvider, VectorStore store,
      Checkpoint checkpoint) throws Exception {
    VectorTriggerService triggers = new VectorTriggerService(connectionProvider);

    // Everything activation creates -- the runtime tables, the sequence, the trigger functions and
    // the triggers -- is declared out of the module's model by excludeFilter.xml, so
    // export.database has nothing to write for any of it; but ad_db_modified computes the checksum
    // inside the database, reads only pg_catalog, and counts them all. A later update.database
    // would then report local changes nobody can export away. Re-stamping settles that, and it is
    // only ours to settle when the structure was already accepted: otherwise the delta holds
    // somebody else's change as well, and catching that is what the check is for.
    //
    // This has to be read before the first statement that can alter the schema, which is the
    // activation and its CREATE TABLE IF NOT EXISTS, not the triggers further down.
    boolean structureWasAccepted = !triggers.isDatabaseModified();

    VectorCapability capability = new VectorActivationService(connectionProvider).activate();
    checkpoint.commit();

    List<Line> lines = new ArrayList<>();
    for (Candidate candidate : candidates) {
      Collection collection = VectorSourceReadiness.collection(connectionProvider, candidate.namespace);
      Verdict verdict = VectorSourceReadiness.verdict(candidate, collection);
      if (verdict == Verdict.COLLECTION_MISSING) {
        // Tenant scope is always on: the search context derives client and organization from the
        // session and never lets a caller supply them, so a collection that did not require them
        // would accept records no search could ever reach.
        store.createCollection(new VectorCollection(candidate.namespace, candidate.dimensions,
            DistanceMetric.valueOf(candidate.metric), true, true));
      }
      // The collection has to exist before the table starts enqueueing into it, and a source that
      // cannot be delivered has to stop enqueueing at all, so this runs after the verdict and
      // follows it either way.
      VectorTriggerService.Deployment deployment = verdict.isUsable()
          ? triggers.deploy(candidate.id)
          : triggers.teardown(candidate.id);
      lines.add(new Line(candidate, collection, verdict, deployment));
    }
    checkpoint.commit();

    // No attempt to decide whether anything actually changed: a run that changed nothing stamps the
    // same checksum again, which is a harmless write, while getting that judgement wrong in the
    // other direction leaves the database reporting changes forever.
    if (structureWasAccepted) {
      triggers.acceptDatabaseStructure();
      checkpoint.commit();
    }
    return new Report(capability, lines);
  }

  // --- turning the report into what the administrator reads -----------------------------------

  private String render(Report report) {
    List<String> outcomes = new ArrayList<>();
    outcomes.add(OBMessageUtils.messageBD("ETARC_VectorActivationState") + " "
        + report.capability.getState() + ". " + report.capability.getDiagnostic());
    for (Line line : report.lines) {
      outcomes.add(line.candidate.name + ": " + render(line) + " " + describe(line.deployment));
    }
    return String.join("\n", outcomes);
  }

  private String render(Line line) {
    String[] params = line.messageParameters();
    return params.length == 0 ? OBMessageUtils.messageBD(line.verdict.getMessageKey())
        : OBMessageUtils.getI18NMessage(line.verdict.getMessageKey(), params);
  }

  /** Says what happened to the change capture, and stays quiet when nothing did. */
  private String describe(VectorTriggerService.Deployment deployment) {
    if (deployment.getInstalled() > 0) {
      return OBMessageUtils.getI18NMessage("ETARC_VectorTriggersInstalled",
          new String[] { String.valueOf(deployment.getInstalled()),
              String.valueOf(deployment.getRemoved()) });
    }
    if (deployment.getRemoved() > 0) {
      return OBMessageUtils.getI18NMessage("ETARC_VectorTriggersRemoved",
          new String[] { String.valueOf(deployment.getRemoved()) });
    }
    return "";
  }

  // --- reading what the run needs -------------------------------------------------------------

  // --- what the run carries -------------------------------------------------------------------

  /** What happened to one source. */
  static final class Line {
    final Candidate candidate;
    final Collection collection;
    final Verdict verdict;
    final VectorTriggerService.Deployment deployment;

    Line(Candidate candidate, Collection collection, Verdict verdict,
        VectorTriggerService.Deployment deployment) {
      this.candidate = candidate;
      this.collection = collection;
      this.verdict = verdict;
      this.deployment = deployment;
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

  /** What a whole run amounted to. */
  static final class Report {
    final VectorCapability capability;
    final List<Line> lines;

    Report(VectorCapability capability, List<Line> lines) {
      this.capability = capability;
      this.lines = lines;
    }

    boolean allReady() {
      return lines.stream().allMatch(line -> line.verdict.isUsable());
    }
  }
}
