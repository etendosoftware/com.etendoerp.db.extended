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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.openbravo.dal.core.OBContext;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

/**
 * Covers the search path, which until now had never run: not in a test, and not by hand either.
 *
 * <p>What it returns is a similarity score the caller filters and ranks on, and a tenant scope
 * that decides which records a user is allowed to see at all. Neither is checked anywhere else,
 * and both fail quietly -- a wrong score just reorders results, and a scope taken from the caller
 * instead of the session returns somebody else's records without any error to notice.</p>
 */
class VectorSearchServiceTest {

  // --- the score --------------------------------------------------------------------------------

  @Test
  void turnsCosineDistanceIntoASimilarityBetweenZeroAndOne() throws Exception {
    // pgvector returns 1 - cosine similarity, so 0 is identical and 2 is opposite.
    assertEquals(1d, scoreOf(DistanceMetric.COSINE, 0d), 1e-9, "an identical vector scores one");
    assertEquals(0.5d, scoreOf(DistanceMetric.COSINE, 1d), 1e-9, "an unrelated vector scores a half");
    assertEquals(0d, scoreOf(DistanceMetric.COSINE, 2d), 1e-9, "an opposite vector scores zero");
  }

  @Test
  void keepsEveryMetricInsideTheSameRangeSoRangesMeanTheSameThing() throws Exception {
    for (DistanceMetric metric : DistanceMetric.values()) {
      for (double distance : new double[] { 0d, 0.5d, 1d, 2d, 50d }) {
        double score = scoreOf(metric, distance);
        assertTrue(score >= 0d && score <= 1d,
            metric + " at distance " + distance + " left the [0,1] range the caller filters on");
      }
    }
  }

  @Test
  void ranksACloserRecordHigherWhateverTheMetric() throws Exception {
    for (DistanceMetric metric : DistanceMetric.values()) {
      assertTrue(scoreOf(metric, 0.2d) > scoreOf(metric, 1.5d),
          metric + " has to score the nearer record higher, or the ranking is inverted");
    }
  }

  // --- what the caller asked for ---------------------------------------------------------------

  @Test
  void refusesAScoreRangeThatCouldNeverMatch() {
    assertRejects("a minimum above the maximum returns nothing and means the caller got it wrong",
        () -> run(matches("a", 0d), List.of("ns"), 5, 0.9d, 0.1d));
    assertRejects("a score below zero is outside the range every metric normalises into",
        () -> run(matches("a", 0d), List.of("ns"), 5, -0.1d, 1d));
    assertRejects("and so is one above one",
        () -> run(matches("a", 0d), List.of("ns"), 5, 0d, 1.5d));
    assertRejects("a range that is not a number would compare false against everything",
        () -> run(matches("a", 0d), List.of("ns"), 5, Double.NaN, 1d));
  }

  @Test
  void refusesToSearchNothing() {
    assertRejects("no namespace means no source to resolve",
        () -> run(matches(), List.of(), 5));
    assertRejects("a blank namespace would resolve to nothing and read as an empty result",
        () -> run(matches(), java.util.Arrays.asList("  "), 5));
  }

  /**
   * The service answers a caller's mistake the same way it answers a database failure: the
   * catch-all wraps everything in VECTOR_SEARCH_OPERATION_FAILED. Asserting the cause is what
   * separates "you asked for something impossible" from "the query blew up".
   */
  private static void assertRejects(String why, org.junit.jupiter.api.function.Executable call) {
    VectorException failure = assertThrows(VectorException.class, call, why);
    assertEquals(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED, failure.getCode());
    assertTrue(failure.getCause() instanceof IllegalArgumentException,
        why + " -- and it has to be the caller's mistake, not a failure of the search itself");
  }

  @Test
  void dropsTheRecordsOutsideTheRequestedScoreRange() throws Exception {
    JSONObject response = new JSONObject(
        run(matches("near", 0.2d, "far", 1.8d), List.of("ns"), 5, 0.5d, 1d));

    JSONArray results = response.getJSONArray("matches");
    assertEquals(1, results.length(), "only the record whose score falls inside the range");
    assertEquals("near", results.getJSONObject(0).getString("id"));
  }

  @Test
  void returnsNoMoreThanTheCallerAskedFor() throws Exception {
    JSONObject response = new JSONObject(
        run(matches("a", 0.1d, "b", 0.2d, "c", 0.3d), List.of("ns"), 2));

    assertEquals(2, response.getJSONArray("matches").length());
  }

  // --- ordering across namespaces ---------------------------------------------------------------

  @Test
  void ordersByDistanceAcrossEveryNamespaceAndNotWithinEachOne() throws Exception {
    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns-a", List.of(match("a-far", 0.9d), match("a-near", 0.1d)));
    store.byNamespace.put("ns-b", List.of(match("b-mid", 0.5d)));

    JSONObject response =
        new JSONObject(run(store, List.of("ns-a", "ns-b"), 5));

    JSONArray results = response.getJSONArray("matches");
    assertEquals(List.of("a-near", "b-mid", "a-far"), idsOf(results),
        "a result from the second namespace has to be able to outrank one from the first");
  }

  @Test
  void namesTheSingleNamespaceItSearchedAndListsThemAllWhenThereAreSeveral() throws Exception {
    JSONObject one = new JSONObject(run(matches("a", 0.1d), List.of("ns"), 5));
    assertEquals("ns", one.getString("namespace"));
    assertEquals(1, one.getJSONArray("namespaces").length());

    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns-a", List.of(match("a", 0.1d)));
    store.byNamespace.put("ns-b", List.of());
    JSONObject several = new JSONObject(run(store, List.of("ns-a", "ns-b"), 5));
    assertFalse(several.has("namespace"), "with several namespaces there is no single one to name");
    assertEquals(2, several.getJSONArray("namespaces").length());
  }

  @Test
  void refusesToRankResultsFromSourcesThatDoNotShareAnEmbeddingProfile() {
    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns-a", List.of());
    store.byNamespace.put("ns-b", List.of());

    VectorException failure = assertThrows(VectorException.class,
        () -> runWithSecondModel(store, "text-embedding-3-large", List.of("ns-a", "ns-b")),
        "distances from two different models are not comparable, so a global order would be a lie");
    assertEquals(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED, failure.getCode());
  }

  // --- tenant scope -----------------------------------------------------------------------------

  @Test
  void takesTheTenantScopeFromTheSessionAndNeverFromTheCaller() throws Exception {
    RecordingStore store = matches("a", 0.1d);

    run(store, List.of("ns"), 5);

    VectorQuery query = store.queries.get(0);
    assertEquals("CLIENT-A", query.getClientId());
    assertEquals(List.of("0", "ORG-A"), List.copyOf(query.getOrganizationIds()),
        "the readable organizations of the session decide what a search may reach; a caller that "
            + "could supply them would read another tenant's records with no error to notice");
  }

  @Test
  void exposesTheIndexedFieldsSeparatelyFromTheRestOfTheMetadata() throws Exception {
    JSONObject response = new JSONObject(run(matches("a", 0.1d), List.of("ns"), 5));

    JSONObject result = response.getJSONArray("matches").getJSONObject(0);
    assertEquals("Widget", result.getJSONObject("fields").getString("name"));
    assertEquals("SRC1", result.getJSONObject("metadata").getString("sourceId"),
        "the whole metadata stays available, the indexed fields are only lifted out of it");
    assertEquals(0.1d, result.getDouble("distance"), 1e-9);
  }

  // --- the target search, which a REST endpoint of Etendo Go reaches -----------------------------

  @Test
  void takesTheTenantScopeFromTheSessionWhenSearchingTargetsToo() throws Exception {
    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns", List.of(match("a", 0.1d)));

    runTargets(store, List.of("catalogue"));

    VectorQuery query = store.queries.get(0);
    assertEquals("CLIENT-A", query.getClientId());
    assertEquals(List.of("0", "ORG-A"), List.copyOf(query.getOrganizationIds()),
        "a target search reaches the same records as any other and may not widen the scope");
  }

  @Test
  void namesTheTargetEachResultCameFrom() throws Exception {
    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns", List.of(match("a", 0.1d)));

    JSONObject response = new JSONObject(runTargets(store, List.of("catalogue")));

    assertEquals(List.of("catalogue"), namesIn(response.getJSONArray("targets")));
    JSONObject result = response.getJSONArray("matches").getJSONObject(0);
    assertEquals("catalogue", result.getString("target"),
        "a caller searching several targets has to know which one answered");
    assertEquals("ns", result.getString("namespace"));
  }

  @Test
  void refusesToSearchNoTargetAtAll() {
    assertRejects("no target key means nothing to resolve", () -> runTargets(matches(), List.of()));
  }

  // --- fixtures ---------------------------------------------------------------------------------

  private static List<String> namesIn(JSONArray array) throws Exception {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      values.add(array.getString(i));
    }
    return values;
  }

  private static List<String> idsOf(JSONArray results) throws Exception {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < results.length(); i++) {
      ids.add(results.getJSONObject(i).getString("id"));
    }
    return ids;
  }

  private static VectorMatch match(String key, double distance) {
    return new VectorMatch(key, "{\"sourceId\":\"SRC1\",\"fields\":{\"name\":\"Widget\"}}", distance);
  }

  /** A store that answers per namespace and remembers what it was asked. */
  private static final class RecordingStore implements VectorStore {
    private final Map<String, List<VectorMatch>> byNamespace = new LinkedHashMap<>();
    private final List<VectorQuery> queries = new ArrayList<>();

    @Override
    public List<VectorMatch> search(VectorQuery query) {
      queries.add(query);
      return byNamespace.getOrDefault(query.getNamespace(), List.of());
    }

    @Override
    public void createCollection(VectorCollection collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void upsert(VectorRecord record) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String namespace, String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteCollection(String namespace) {
      throw new UnsupportedOperationException();
    }
  }

  private static RecordingStore matches(Object... keysAndDistances) {
    RecordingStore store = new RecordingStore();
    List<VectorMatch> found = new ArrayList<>();
    for (int i = 0; i < keysAndDistances.length; i += 2) {
      found.add(match((String) keysAndDistances[i], (Double) keysAndDistances[i + 1]));
    }
    store.byNamespace.put("ns", found);
    return store;
  }

  /** Runs a search with the session, the sources and the provider all stood in for. */
  private String searchWith(RecordingStore store, String secondModel, DistanceMetric metric,
      java.util.function.Function<VectorSearchService, String> call) {
    OBContext obContext = mock(OBContext.class);
    Client client = mock(Client.class);
    Organization organization = mock(Organization.class);
    when(client.getId()).thenReturn("CLIENT-A");
    when(organization.getId()).thenReturn("ORG-A");
    when(obContext.getCurrentClient()).thenReturn(client);
    when(obContext.getCurrentOrganization()).thenReturn(organization);
    when(obContext.getReadableOrganizations()).thenReturn(new String[] { "0", "ORG-A" });

    try (MockedStatic<OBContext> contextApi = mockStatic(OBContext.class)) {
      contextApi.when(OBContext::getOBContext).thenReturn(obContext);
      return call.apply(service(store, secondModel, metric));
    }
  }

  private VectorSearchService service(RecordingStore store, String secondModel, DistanceMetric metric) {
    try {
      int[] resolved = { 0 };
      ConnectionProvider cp = mock(ConnectionProvider.class);
      when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
        String sql = invocation.getArgument(0);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        if (sql.contains("FROM etarc_vector_source s")) {
          when(rows.next()).thenReturn(true);
          when(rows.getString(1)).thenReturn("SRC1");
          when(rows.getString(2)).thenAnswer(i -> namespaceOf(store, resolved[0]));
          when(rows.getString(3)).thenReturn(metric.name());
          when(rows.getString(4)).thenReturn("OPENAI");
          // The second source answers with another model when the test asks for one, which is what
          // makes the two profiles incompatible.
          when(rows.getString(5)).thenAnswer(i ->
              resolved[0]++ == 0 || secondModel == null ? "text-embedding-3-small" : secondModel);
          when(rows.getInt(6)).thenReturn(1536);
        } else if (sql.contains("FROM etarc_vector_search_target t")) {
          when(rows.next()).thenReturn(true);
          when(rows.getString(1)).thenReturn("SRC1");
          when(rows.getString(2)).thenAnswer(i -> namespaceOf(store, resolved[0]));
          when(rows.getString(3)).thenReturn(null);
        } else {
          when(rows.next()).thenReturn(false);
        }
        when(statement.executeQuery()).thenReturn(rows);
        return statement;
      });
      return new VectorSearchService(cp, store, fixedProvider(cp));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String namespaceOf(RecordingStore store, int index) {
    List<String> namespaces = List.copyOf(store.byNamespace.keySet());
    return namespaces.get(Math.min(index, namespaces.size() - 1));
  }

  /** Stands in for the provider: every real one reaches an external service on embed. */
  private static VectorEmbeddingProviderFactory fixedProvider(ConnectionProvider cp) {
    return new VectorEmbeddingProviderFactory(cp) {
      @Override
      VectorEmbeddingProvider forSource(String sourceId) {
        return new VectorEmbeddingProvider() {
          @Override
          public List<double[]> embed(List<String> texts) {
            return texts.stream().map(t -> new double[] { 0.1d, 0.2d }).toList();
          }

          @Override
          public int batchSize() {
            return 1;
          }

          @Override
          public int dimensions() {
            return 2;
          }
        };
      }
    };
  }

  /** Runs a search with the defaults most tests want. */
  private String run(RecordingStore store, java.util.Collection<String> namespaces, int topK) {
    return run(store, namespaces, topK, 0d, 1d);
  }

  private String run(RecordingStore store, java.util.Collection<String> namespaces, int topK,
      double minScore, double maxScore) {
    return searchWith(store, null, DistanceMetric.COSINE,
        s -> s.searchAsJson(namespaces, "q", topK, "{}", minScore, maxScore));
  }

  private String runTargets(RecordingStore store, java.util.Collection<String> targetKeys) {
    return searchWith(store, null, DistanceMetric.COSINE,
        s -> s.searchTargetsAsJson(targetKeys, "q", 5, 0d, 1d));
  }

  private String runWithSecondModel(RecordingStore store, String secondModel,
      java.util.Collection<String> namespaces) {
    return searchWith(store, secondModel, DistanceMetric.COSINE,
        s -> s.searchAsJson(namespaces, "q", 5, "{}", 0d, 1d));
  }

  private double scoreOf(DistanceMetric metric, double distance) throws Exception {
    RecordingStore store = new RecordingStore();
    store.byNamespace.put("ns", List.of(match("only", distance)));
    String json = searchWith(store, null, metric,
        s -> s.searchAsJson(List.of("ns"), "q", 5, "{}", 0d, 1d));
    return new JSONObject(json).getJSONArray("matches").getJSONObject(0).getDouble("score");
  }
}
