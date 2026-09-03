package com.etendoerp.db.extended.vector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.database.ConnectionProvider;

/**
 * Generic semantic-search facade. It resolves the source provider from the Application Dictionary
 * configuration and exposes indexed fields as JSON without depending on an Etendo entity.
 */
public final class VectorSearchService {
  private final ConnectionProvider connectionProvider;
  private final VectorStore vectorStore;
  private final VectorEmbeddingProviderFactory providers;
  private final VectorSearchSourceResolver sources;
  private final VectorSearchTargetResolver targets;

  public VectorSearchService(ConnectionProvider connectionProvider) {
    this(connectionProvider, new VectorStoreService(connectionProvider));
  }

  VectorSearchService(ConnectionProvider connectionProvider, VectorStore vectorStore) {
    this.connectionProvider = connectionProvider;
    this.vectorStore = vectorStore;
    this.providers = new VectorEmbeddingProviderFactory(connectionProvider);
    this.sources = new VectorSearchSourceResolver(connectionProvider);
    this.targets = new VectorSearchTargetResolver(connectionProvider);
  }

  /**
   * Embeds {@code text}, finds the nearest records and returns a portable JSON response.
   */
  public String searchAsJson(String namespace, String text, int topK, String metadataFilter) {
    return searchAsJson(Collections.singletonList(namespace), text, topK, metadataFilter);
  }

  /**
   * Searches the configured namespaces selected by the caller. All sources must use the same
   * provider type, model, dimensions, and distance metric so a global result order is meaningful.
   * Tenant scope always comes from the active Etendo context.
   */
  public String searchAsJson(Collection<String> namespaces, String text, int topK,
      String metadataFilter) {
    return searchAsJson(namespaces, text, topK, metadataFilter, 0d, 1d);
  }

  /** Searches configured target keys. Target Display Logic is always applied before results leave the service. */
  public String searchTargetsAsJson(Collection<String> targetKeys, String text, int topK,
      double minScore, double maxScore) {
    try {
      validateScoreRange(minScore, maxScore);
      if (targetKeys == null || targetKeys.isEmpty()) throw new IllegalArgumentException("At least one vector target is required.");
      VectorSearchContext context = VectorSearchContext.current();
      List<VectorSearchTarget> configuredTargets = new ArrayList<>();
      for (String key : targetKeys) configuredTargets.add(targets.resolve(key));
      VectorSearchSource first = configuredTargets.get(0).getSource();
      List<VectorSearchSource> configuredSources = new ArrayList<>();
      for (VectorSearchTarget target : configuredTargets) configuredSources.add(target.getSource());
      ensureCompatibleProfiles(first, configuredSources);
      double[] embedding = providers.forSource(first.getId()).embed(text);
      List<TargetMatch> matches = new ArrayList<>();
      for (VectorSearchTarget target : configuredTargets) {
        VectorSearchSource source = target.getSource();
        for (String organizationId : context.getOrganizationIds()) {
          VectorQuery query = new VectorQuery(source.getNamespace(), embedding, topK, source.getMetric(), "{}", target.getFilter(), context.getClientId(), organizationId);
          for (VectorMatch match : vectorStore.search(query)) {
            double score = scoreFor(match.getDistance(), source.getMetric());
            if (score >= minScore && score <= maxScore) matches.add(new TargetMatch(target, match, score));
          }
        }
      }
      matches.sort(Comparator.comparingDouble(TargetMatch::getDistance));
      JSONObject response = new JSONObject();
      JSONArray searchedTargets = new JSONArray();
      for (VectorSearchTarget target : configuredTargets) searchedTargets.put(target.getKey());
      response.put("targets", searchedTargets);
      JSONArray results = new JSONArray();
      for (TargetMatch match : matches.subList(0, Math.min(topK, matches.size()))) {
        VectorMatch vectorMatch = match.getMatch();
        JSONObject metadata = new JSONObject(vectorMatch.getMetadata());
        JSONObject result = new JSONObject();
        result.put("target", match.getTarget().getKey());
        result.put("namespace", match.getTarget().getSource().getNamespace());
        result.put("id", vectorMatch.getKey());
        result.put("distance", vectorMatch.getDistance());
        result.put("score", match.getScore());
        result.put("fields", metadata.optJSONObject("fields"));
        result.put("metadata", metadata);
        results.put(result);
      }
      response.put("matches", results);
      return response.toString();
    } catch (VectorException e) { throw e;
    } catch (Exception e) { throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED, "Could not execute target vector search.", e); }
  }

  /**
   * Searches configured namespaces and retains only normalized similarity scores in the requested
   * inclusive range. Scores are always normalized to the [0, 1] interval.
   */
  public String searchAsJson(Collection<String> namespaces, String text, int topK,
      String metadataFilter, double minScore, double maxScore) {
    try {
      validateScoreRange(minScore, maxScore);
      VectorSearchContext context = VectorSearchContext.current();
      List<VectorSearchSource> configuredSources = resolveSources(namespaces);
      VectorSearchSource first = configuredSources.get(0);
      ensureCompatibleProfiles(first, configuredSources);
      VectorEmbeddingProvider provider = providers.forSource(first.getId());
      double[] embedding = provider.embed(text);
      List<NamespacedMatch> matches = new ArrayList<>();
      for (VectorSearchSource source : configuredSources) {
        for (String organizationId : context.getOrganizationIds()) {
          for (VectorMatch match : vectorStore.search(new VectorQuery(source.getNamespace(), embedding,
              topK, source.getMetric(), metadataFilter, context.getClientId(), organizationId))) {
            double score = scoreFor(match.getDistance(), source.getMetric());
            if (score >= minScore && score <= maxScore) {
              matches.add(new NamespacedMatch(source.getNamespace(), match, score));
            }
          }
        }
      }
      matches.sort(Comparator.comparingDouble(NamespacedMatch::getDistance));
      JSONObject response = new JSONObject();
      JSONArray searchedNamespaces = new JSONArray();
      for (VectorSearchSource source : configuredSources) searchedNamespaces.put(source.getNamespace());
      response.put("namespaces", searchedNamespaces);
      if (configuredSources.size() == 1) {
        response.put("namespace", first.getNamespace());
      }
      JSONArray results = new JSONArray();
      for (NamespacedMatch match : matches.subList(0, Math.min(topK, matches.size()))) {
        VectorMatch vectorMatch = match.getMatch();
        JSONObject metadata = new JSONObject(vectorMatch.getMetadata());
        JSONObject result = new JSONObject();
        result.put("namespace", match.getNamespace());
        result.put("id", vectorMatch.getKey());
        result.put("distance", vectorMatch.getDistance());
        result.put("score", match.getScore());
        result.put("fields", metadata.optJSONObject("fields"));
        result.put("metadata", metadata);
        results.put(result);
      }
      response.put("matches", results);
      return response.toString();
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
          "Could not execute semantic vector search.", e);
    }
  }

  private List<VectorSearchSource> resolveSources(Collection<String> namespaces) {
    if (namespaces == null || namespaces.isEmpty()) {
      throw new IllegalArgumentException("At least one vector namespace is required.");
    }
    List<VectorSearchSource> result = new ArrayList<>();
    for (String namespace : namespaces) {
      if (namespace == null || namespace.trim().isEmpty()) {
        throw new IllegalArgumentException("Vector namespace is required.");
      }
      result.add(sources.resolve(namespace));
    }
    return result;
  }

  private static void ensureCompatibleProfiles(VectorSearchSource first,
      List<VectorSearchSource> configuredSources) {
    for (VectorSearchSource source : configuredSources) {
      if (!first.hasCompatibleEmbeddingProfile(source)) {
        throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
            "Global semantic search requires compatible source embedding profiles.");
      }
    }
  }

  private static void validateScoreRange(double minScore, double maxScore) {
    if (!Double.isFinite(minScore) || !Double.isFinite(maxScore) || minScore < 0d || maxScore > 1d
        || minScore > maxScore) {
      throw new IllegalArgumentException("Score range must be within 0 and 1.");
    }
  }

  private static double scoreFor(double distance, DistanceMetric metric) {
    switch (metric) {
      case COSINE:
        return clamp((2d - distance) / 2d);
      case L2:
        return 1d / (1d + Math.max(0d, distance));
      case INNER_PRODUCT:
        return 1d / (1d + Math.exp(distance));
      default:
        throw new IllegalArgumentException("Unsupported vector distance metric.");
    }
  }

  private static double clamp(double value) {
    return Math.max(0d, Math.min(1d, value));
  }

  private static final class NamespacedMatch {
    private final String namespace;
    private final VectorMatch match;
    private final double score;

    private NamespacedMatch(String namespace, VectorMatch match, double score) {
      this.namespace = namespace;
      this.match = match;
      this.score = score;
    }

    private String getNamespace() { return namespace; }
    private VectorMatch getMatch() { return match; }
    private double getDistance() { return match.getDistance(); }
    private double getScore() { return score; }
  }

  private static final class TargetMatch {
    private final VectorSearchTarget target; private final VectorMatch match; private final double score;
    private TargetMatch(VectorSearchTarget target, VectorMatch match, double score) { this.target = target; this.match = match; this.score = score; }
    private VectorSearchTarget getTarget() { return target; } private VectorMatch getMatch() { return match; }
    private double getDistance() { return match.getDistance(); } private double getScore() { return score; }
  }
}
