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

import org.openbravo.database.ConnectionProvider;

import com.etendoerp.db.extended.data.VectorEmbedProvider;
import com.etendoerp.db.extended.data.VectorSource;

/**
 * Whether a source can be indexed as it stands, and why not when it cannot.
 *
 * <p>Both windows that act on a source ask the same question. Activation asks it to decide whether
 * to create a collection and instrument the table; a reindex request asks it because enqueueing a
 * whole table for a source that cannot be delivered produces as many failures as the table has
 * rows. One definition means the two can never disagree about what ready means, and the verdict
 * an administrator reads is the same in both places.</p>
 */
final class VectorSourceReadiness {

  /** The collection as stored, so it can be compared with what the source now asks for. */
  private static final String COLLECTION_SQL =
      "SELECT dimensions, metric FROM etarc_vector_collection WHERE namespace = ?";

  /**
   * Counts the columns exactly as the consumer reads them, so nothing can call a source ready when
   * delivery would reject it.
   *
   * @see DictionaryVectorOutboxConsumer
   */
  private static final String COLUMN_COUNT_SQL =
      "SELECT count(*), count(*) FILTER (WHERE sc.iscontent = 'Y') "
          + "FROM etarc_vector_source_column sc "
          + "WHERE sc.etarc_vector_source_id = ? AND sc.isactive = 'Y'";

  private VectorSourceReadiness() {
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
    if (candidate.dimensions == null) {
      return Verdict.WITHOUT_PROVIDER;
    }
    if (candidate.columns == 0) {
      return Verdict.WITHOUT_COLUMNS;
    }
    if (candidate.contentColumns == 0) {
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

  /** Reads a source into the plain values a verdict is made of, detached from the DAL session. */
  static Candidate candidate(ConnectionProvider connectionProvider, VectorSource source) throws Exception {
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
    return new Candidate(source.getId(), source.getName(), source.getNamespace(),
        source.getDistanceMetric(), Boolean.TRUE.equals(source.isEnabled()),
        provider == null ? null : provider.getDimensions().intValue(), columns, contentColumns);
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
    WITHOUT_PROVIDER("ETARC_VectorSourceWithoutProvider"),
    WITHOUT_COLUMNS("ETARC_VectorSourceWithoutColumns"),
    WITHOUT_CONTENT("ETARC_VectorSourceWithoutContent"),
    COLLECTION_MISSING("ETARC_VectorCollectionCreated"),
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
     * Whether the source can be indexed as it stands.
     *
     * <p>A missing collection counts as usable because activation creates it on the spot; every
     * other refusal needs an administrator to change the configuration first.</p>
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
    final int columns;
    final int contentColumns;

    Candidate(String id, String name, String namespace, String metric, boolean enabled,
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
  static final class Collection {
    final int dimensions;
    final String metric;

    Collection(int dimensions, String metric) {
      this.dimensions = dimensions;
      this.metric = metric;
    }
  }
}
