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
package com.etendoerp.db.extended.utils.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.openbravo.database.ConnectionProvider;

/**
 * Whether a source can be indexed as it stands, and why not when it cannot.
 *
 * <p>Both windows that act on a source ask the same question. Activation asks it to decide whether
 * to create a collection and instrument the table; a reindex request asks it because enqueueing a
 * whole table for a source that cannot be delivered produces as many failures as the table has
 * rows. One definition means the two can never disagree about what ready means, and the verdict
 * an administrator reads is the same in both places.</p>
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.VectorSourceReadiness} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 */
final class VectorSourceReadiness {

  /** The collection as stored, so it can be compared with what the source now asks for. */
  private static final String COLLECTION_SQL =
      "SELECT dimensions, metric FROM etarc_vector.etarc_vector_collection WHERE namespace = ?";

  /**
   * Every configured source, read over JDBC rather than through the DAL.
   *
   * <p>The post-update script provisions from the same definition the window reports, and a module
   * script has no DAL session to read entities with. Keeping one query means the update and the
   * window can never disagree about what a source is.</p>
   */
  private static final String ALL_SOURCES_SQL =
      "SELECT s.etarc_vector_source_id, s.name, s.namespace, s.distance_metric, "
          + "  s.isactive, s.isenabled, p.dimensions, "
          + "  (SELECT count(*) FROM etarc_vector_source_column sc "
          + "    WHERE sc.etarc_vector_source_id = s.etarc_vector_source_id "
          + "      AND sc.isactive = 'Y') AS columns, "
          + "  (SELECT count(*) FROM etarc_vector_source_column sc "
          + "    WHERE sc.etarc_vector_source_id = s.etarc_vector_source_id "
          + "      AND sc.isactive = 'Y' AND sc.iscontent = 'Y') AS content_columns, "
          + "  (SELECT count(*) FROM ad_column k "
          + "    WHERE k.ad_table_id = s.ad_table_id "
          + "      AND k.iskey = 'Y' AND k.isactive = 'Y') AS key_columns "
          + "FROM etarc_vector_source s "
          + "LEFT JOIN etarc_vector_embed_provider p "
          + "  ON p.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
          + " AND p.isactive = 'Y' "
          + "ORDER BY s.etarc_vector_source_id";

  private VectorSourceReadiness() {
  }

  /**
   * Reads every configured source into the plain values a verdict is made of.
   *
   * @param connectionProvider
   *     connection the sources are read with
   * @return one candidate per configured source
   * @throws Exception
   *     if the sources cannot be read
   */
  static List<Candidate> candidates(ConnectionProvider connectionProvider) throws Exception {
    List<Candidate> candidates = new ArrayList<>();
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(ALL_SOURCES_SQL);
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        // wasNull answers for the column read last, so it is asked before anything else is read:
        // a source without a provider has no dimensions, and that is its own verdict.
        int width = result.getInt("dimensions");
        Integer dimensions = result.wasNull() ? null : Integer.valueOf(width);
        boolean enabled = "Y".equals(result.getString("isactive"))
            && "Y".equals(result.getString("isenabled"));
        candidates.add(new Candidate(result.getString("etarc_vector_source_id"),
            result.getString("name"), result.getString("namespace"),
            result.getString("distance_metric"), enabled, dimensions,
            new Columns(result.getInt("columns"), result.getInt("content_columns"),
                result.getInt("key_columns") > 0)));
      }
    }
    return candidates;
  }

  /**
   * Decides what one source is, given what it asks for and what its collection currently holds.
   *
   * <p>A pure decision on purpose. It is the part worth being sure about -- every branch is a way
   * a source can be silently useless -- and it needs neither a database nor a dictionary to be
   * stated.</p>
   *
   * @param candidate the source as configured
   * @param collection its collection as stored, or {@code null} when it has none yet
   */
  static Verdict verdict(Candidate candidate, Collection collection) {
    if (!candidate.enabled) {
      return Verdict.DISABLED;
    }
    // Before anything about the source itself: the trigger writes the record's key into the queue,
    // so without one there is nothing to instrument and no configuration would make there be.
    if (!candidate.columns.key) {
      return Verdict.WITHOUT_KEY;
    }
    if (candidate.dimensions == null) {
      return Verdict.WITHOUT_PROVIDER;
    }
    if (candidate.columns.total == 0) {
      return Verdict.WITHOUT_COLUMNS;
    }
    if (candidate.columns.content == 0) {
      return Verdict.WITHOUT_CONTENT;
    }
    if (collection == null) {
      return Verdict.COLLECTION_MISSING;
    }
    if (collection.dimensions != candidate.dimensions.intValue()) {
      return Verdict.DIMENSION_DRIFT;
    }
    if (!collection.metric.equals(candidate.metric)) {
      return Verdict.METRIC_DRIFT;
    }
    return Verdict.READY;
  }

  static Collection collection(ConnectionProvider connectionProvider, String namespace) throws Exception {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(COLLECTION_SQL)) {
      statement.setString(1, namespace);
      try (ResultSet result = statement.executeQuery()) {
        return result.next() ? new Collection(result.getInt(1), result.getString(2)) : null;
      }
    }
  }

  /** What a source turned out to be. Whether each verdict is actionable is the caller's business. */
  enum Verdict {
    DISABLED("ETARC_VectorSourceDisabled"),
    WITHOUT_KEY("ETARC_VectorSourceWithoutKey"),
    WITHOUT_PROVIDER("ETARC_VectorSourceWithoutProvider"),
    WITHOUT_COLUMNS("ETARC_VectorSourceWithoutColumns"),
    WITHOUT_CONTENT("ETARC_VectorSourceWithoutContent"),
    COLLECTION_MISSING("ETARC_VectorSourceNotProvisioned"),
    DIMENSION_DRIFT("ETARC_VectorCollectionDimensionDrift"),
    METRIC_DRIFT("ETARC_VectorCollectionMetricDrift"),
    READY("ETARC_VectorSourceAlreadyActive");

    private final String messageKey;

    Verdict(String messageKey) {
      this.messageKey = messageKey;
    }

    String getMessageKey() {
      return messageKey;
    }

    /**
     * Whether the source is configured well enough to be indexed.
     *
     * <p>A missing collection counts as usable because the next update creates it; every other
     * refusal needs an administrator to change the configuration first.</p>
     */
    boolean isUsable() {
      return this == READY || this == COLLECTION_MISSING;
    }
  }

  /** What one source contributes to a decision, detached from the DAL session. */
  static final class Candidate {
    final String id;
    final String name;
    final String namespace;
    final String metric;
    final boolean enabled;
    final Integer dimensions;
    final Columns columns;

    Candidate(String id, String name, String namespace, String metric, boolean enabled,
        Integer dimensions, Columns columns) {
      this.id = id;
      this.name = name;
      this.namespace = namespace;
      this.metric = metric;
      this.enabled = enabled;
      this.dimensions = dimensions;
      this.columns = columns;
    }
  }

  /**
   * What the columns of a source amount to: how many it configures, how many carry content, and
   * whether the table it indexes has a key column to identify a record by.
   *
   * <p>All three are read together and by adjacent checks, and each is its own verdict: a source
   * with columns but no content is not the same as one with no columns, and neither is a table
   * whose records the queue could not name.</p>
   */
  static final class Columns {
    final int total;
    final int content;
    final boolean key;

    Columns(int total, int content, boolean key) {
      this.total = total;
      this.content = content;
      this.key = key;
    }
  }

  /** The stored shape of a collection, which a source can no longer agree with. */
  static final class Collection {
    final int dimensions;
    final String metric;

    Collection(int dimensions, String metric) {
      this.dimensions = dimensions;
      this.metric = metric;
    }
  }
}
