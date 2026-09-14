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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import com.etendoerp.db.extended.data.VectorEmbedProvider;
import com.etendoerp.db.extended.data.VectorSource;
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
 * and it takes several records at a time so one source, a few, or all of them can be activated in
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

  /** The collection as stored, so it can be compared with what the source now asks for. */
  private static final String COLLECTION_SQL =
      "SELECT dimensions, metric FROM etarc_vector_collection WHERE namespace = ?";

  /**
   * Counts the columns exactly as the consumer reads them, so the button cannot call a source
   * ready when delivery would reject it.
   *
   * @see DictionaryVectorOutboxConsumer
   */
  private static final String COLUMN_COUNT_SQL =
      "SELECT count(*), count(*) FILTER (WHERE sc.iscontent = 'Y') "
          + "FROM etarc_vector_source_column sc "
          + "WHERE sc.etarc_vector_source_id = ? AND sc.isactive = 'Y'";

  @Override
  protected Class<?> getInputClass() {
    return VectorSource.class;
  }

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    ActionResult result = new ActionResult();
    List<String> outcomes = new ArrayList<>();
    try {
      ConnectionProvider connectionProvider = new DalConnectionProvider(false);

      // Read everything the run needs while the records are still attached. Activation commits and
      // closes the DAL session, and an entity read after that is detached.
      List<Candidate> candidates = new ArrayList<>();
      for (VectorSource source : getInputContents(VectorSource.class)) {
        candidates.add(candidate(connectionProvider, source));
      }

      VectorCapability capability = new VectorActivationService(connectionProvider).activate();
      OBDal.getInstance().commitAndClose();
      outcomes.add(OBMessageUtils.messageBD("ETARC_VectorActivationState") + " "
          + capability.getState() + ". " + capability.getDiagnostic());

      VectorStore store = new VectorStoreService(connectionProvider);
      VectorTriggerService triggers = new VectorTriggerService(connectionProvider);
      boolean allReady = true;
      for (Candidate candidate : candidates) {
        Outcome outcome = activate(connectionProvider, store, candidate);
        allReady &= outcome.ready;
        // The collection has to exist before the table starts enqueueing into it, and a source
        // that cannot be delivered has to stop enqueueing at all, so this runs after the verdict
        // and follows it either way.
        VectorTriggerService.Deployment deployment = outcome.ready
            ? triggers.deploy(candidate.id)
            : triggers.teardown(candidate.id);
        outcomes.add(candidate.name + ": " + outcome.message + " " + describe(deployment));
      }
      OBDal.getInstance().commitAndClose();

      // A run that left a source unusable is not a success, however well the bootstrap went.
      result.setType(allReady ? Result.Type.SUCCESS : Result.Type.WARNING);
      result.setMessage(String.join("\n", outcomes));
    } catch (Exception e) {
      log.error("Vector source activation failed", e);
      OBDal.getInstance().rollbackAndClose();
      outcomes.add(e.getMessage());
      result.setType(Result.Type.ERROR);
      result.setMessage(String.join("\n", outcomes));
    }
    return result;
  }

  /**
   * Creates the collection of one source, or explains what is stopping it.
   *
   * <p>A source is reported rather than rejected so that selecting every row stays a sensible way
   * to use the button: one broken source in the selection must not stop the rest.</p>
   */
  private Outcome activate(ConnectionProvider connectionProvider, VectorStore store, Candidate candidate)
      throws Exception {
    if (!candidate.enabled) {
      return Outcome.notReady("ETARC_VectorSourceDisabled");
    }
    if (candidate.dimensions == null) {
      return Outcome.notReady("ETARC_VectorSourceWithoutProvider");
    }
    if (candidate.columns == 0) {
      return Outcome.notReady("ETARC_VectorSourceWithoutColumns");
    }
    if (candidate.contentColumns == 0) {
      return Outcome.notReady("ETARC_VectorSourceWithoutContent");
    }

    Collection collection = collection(connectionProvider, candidate.namespace);
    if (collection == null) {
      // Tenant scope is always on: the search context derives client and organization from the
      // session and never lets a caller supply them, so a collection that did not require them
      // would accept records no search could ever reach.
      store.createCollection(new VectorCollection(candidate.namespace, candidate.dimensions,
          DistanceMetric.valueOf(candidate.metric), true, true));
      return Outcome.ready("ETARC_VectorCollectionCreated");
    }
    if (collection.dimensions != candidate.dimensions.intValue()) {
      return Outcome.notReadyWith(OBMessageUtils.getI18NMessage("ETARC_VectorCollectionDimensionDrift",
          new String[] { String.valueOf(collection.dimensions), String.valueOf(candidate.dimensions) }));
    }
    if (!collection.metric.equals(candidate.metric)) {
      return Outcome.notReadyWith(OBMessageUtils.getI18NMessage("ETARC_VectorCollectionMetricDrift",
          new String[] { collection.metric, candidate.metric }));
    }
    return Outcome.ready("ETARC_VectorSourceAlreadyActive");
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

  private Candidate candidate(ConnectionProvider connectionProvider, VectorSource source) throws Exception {
    VectorEmbedProvider provider = source.getEtarcVectorEmbedProvider();
    int columns = 0;
    int contentColumns = 0;
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(COLUMN_COUNT_SQL)) {
      statement.setString(1, source.getId());
      try (ResultSet result = statement.executeQuery()) {
        if (result.next()) {
          columns = result.getInt(1);
          contentColumns = result.getInt(2);
        }
      }
    }
    return new Candidate(source.getId(), source.getName(), source.getNamespace(), source.getDistanceMetric(),
        Boolean.TRUE.equals(source.isEnabled()),
        provider == null ? null : provider.getDimensions().intValue(), columns, contentColumns);
  }

  private Collection collection(ConnectionProvider connectionProvider, String namespace) throws Exception {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(COLLECTION_SQL)) {
      statement.setString(1, namespace);
      try (ResultSet result = statement.executeQuery()) {
        return result.next() ? new Collection(result.getInt(1), result.getString(2)) : null;
      }
    }
  }

  /** What one selected source contributes to the run, detached from the DAL session. */
  private static final class Candidate {
    private final String id;
    private final String name;
    private final String namespace;
    private final String metric;
    private final boolean enabled;
    private final Integer dimensions;
    private final int columns;
    private final int contentColumns;

    private Candidate(String id, String name, String namespace, String metric, boolean enabled,
        Integer dimensions, int columns, int contentColumns) {
      this.id = id;
      this.name = name;
      this.namespace = namespace;
      this.metric = metric;
      this.enabled = enabled;
      this.dimensions = dimensions;
      this.columns = columns;
      this.contentColumns = contentColumns;
    }
  }

  /** The stored shape of a collection, which a source can no longer agree with. */
  private static final class Collection {
    private final int dimensions;
    private final String metric;

    private Collection(int dimensions, String metric) {
      this.dimensions = dimensions;
      this.metric = metric;
    }
  }

  /** What happened to one source, and whether it can now be indexed. */
  private static final class Outcome {
    private final String message;
    private final boolean ready;

    private Outcome(String message, boolean ready) {
      this.message = message;
      this.ready = ready;
    }

    private static Outcome ready(String messageKey) {
      return new Outcome(OBMessageUtils.messageBD(messageKey), true);
    }

    private static Outcome notReady(String messageKey) {
      return new Outcome(OBMessageUtils.messageBD(messageKey), false);
    }

    /** For a message whose parameters were already interpolated by the caller. */
    private static Outcome notReadyWith(String message) {
      return new Outcome(message, false);
    }
  }
}
