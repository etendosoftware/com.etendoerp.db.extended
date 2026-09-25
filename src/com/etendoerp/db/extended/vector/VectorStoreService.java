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

import java.sql.PreparedStatement; import java.sql.ResultSet; import java.util.ArrayList; import java.util.List;
import org.openbravo.database.ConnectionProvider;

/** JDBC implementation that permits operations only after explicit activation. */
public class VectorStoreService implements VectorStore {
  private final ConnectionProvider cp; private final VectorCapabilityService capability;
  private boolean activationVerified;
  public VectorStoreService(ConnectionProvider cp) { this.cp = cp; capability = new VectorCapabilityService(cp); }
  // SYNC: createCollection and requireActive are duplicated in
  // src-util/modulescript/src/com/etendoerp/db/extended/utils/vector/VectorProvisioningService.java
  // (createCollection), used by the GenerateVectorSourceTriggers post-update script. Remember to
  // apply any change here to that copy too.
  public void createCollection(VectorCollection collection) {
    requireActive(); String sql = "INSERT INTO etarc_vector.etarc_vector_collection (namespace, dimensions, metric, client_scoped, organization_scoped) VALUES (?, ?, ?, ?, ?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) { ps.setString(1, collection.getNamespace()); ps.setInt(2, collection.getDimensions()); ps.setString(3, collection.getMetric().name()); ps.setBoolean(4, collection.isClientScoped()); ps.setBoolean(5, collection.isOrganizationScoped()); ps.executeUpdate(); } catch (Exception e) { throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, "Could not create vector collection.", e); }
  }
  public void upsert(VectorRecord vectorRecord) {
    requireActive();
    CollectionInfo c = collection(vectorRecord.getNamespace());
    if (c.dimensions != vectorRecord.getVector().length) {
      throw new VectorException(VectorErrorCode.VECTOR_DIMENSION_MISMATCH,
          "Vector dimension does not match the collection.");
    }
    validateScope(c, vectorRecord.getClientId(), vectorRecord.getOrganizationId());
    validateMetadata(vectorRecord.getMetadata());
    String sql = "INSERT INTO etarc_vector.etarc_vector_record (namespace, external_key, client_id,"
        + " organization_id, embedding, metadata)"
        + " VALUES (?, ?, ?, ?, ?::vector, ?::jsonb)"
        + " ON CONFLICT (namespace, external_key, client_id, organization_id)"
        + " DO UPDATE SET embedding = EXCLUDED.embedding, metadata = EXCLUDED.metadata,"
        + " updated_at = now()";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, vectorRecord.getNamespace());
      ps.setString(2, vectorRecord.getKey());
      ps.setString(3, scopeValue(vectorRecord.getClientId()));
      ps.setString(4, scopeValue(vectorRecord.getOrganizationId()));
      ps.setString(5, literal(vectorRecord.getVector()));
      ps.setString(6, vectorRecord.getMetadata());
      ps.executeUpdate();
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,
          "Could not store vector record.", e);
    }
  }
  public List<VectorMatch> search(VectorQuery q) {
    requireActive();
    CollectionInfo c = collection(q.getNamespace());
    if (c.dimensions != q.getVector().length) {
      throw new VectorException(VectorErrorCode.VECTOR_DIMENSION_MISMATCH, "Vector dimension does not match the collection.");
    }
    validateScope(c, q.getClientId(), q.getOrganizationIds());
    validateMetadata(q.getMetadata());
    List<String> organizationIds = q.getOrganizationIds();
    // One placeholder per organization instead of one query per organization.
    String organizationClause = organizationIds.isEmpty() ? ""
        : " AND organization_id IN (" + String.join(", ", java.util.Collections.nCopies(organizationIds.size(), "?")) + ")";
    String sql = "SELECT external_key, metadata::text, embedding " + q.getMetric().getOperator()
        + " ?::vector AS distance FROM etarc_vector.etarc_vector_record WHERE namespace = ? AND "
        + q.getMetadataFilter().getClause() + " AND (? IS NULL OR client_id = ?)" + organizationClause
        + " ORDER BY embedding " + q.getMetric().getOperator() + " ?::vector LIMIT ?";
    List<VectorMatch> result = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      String v = literal(q.getVector());
      int parameter = 1;
      ps.setString(parameter++, v);
      ps.setString(parameter++, q.getNamespace());
      parameter = q.getMetadataFilter().bind(ps, parameter);
      ps.setString(parameter++, q.getClientId());
      ps.setString(parameter++, q.getClientId());
      for (String organizationId : organizationIds) {
        ps.setString(parameter++, organizationId);
      }
      ps.setString(parameter++, v);
      ps.setInt(parameter, q.getTopK());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) result.add(new VectorMatch(rs.getString(1), rs.getString(2), rs.getDouble(3)));
      }
      return result;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED, "Could not search vector records.", e);
    }
  }
  public void delete(String namespace, String key) { requireActive(); try(PreparedStatement ps=cp.getPreparedStatement("DELETE FROM etarc_vector.etarc_vector_record WHERE namespace = ? AND external_key = ?")){ps.setString(1,namespace);ps.setString(2,key);ps.executeUpdate();}catch(Exception e){throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,"Could not delete vector record.",e);} }
  public void deleteCollection(String namespace) { requireActive(); try(PreparedStatement ps=cp.getPreparedStatement("DELETE FROM etarc_vector.etarc_vector_collection WHERE namespace = ?")){ps.setString(1,namespace);ps.executeUpdate();}catch(Exception e){throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,"Could not delete vector collection.",e);} }
  // SYNC: duplicated in the createCollection of
  // src-util/modulescript/src/com/etendoerp/db/extended/utils/vector/VectorProvisioningService.java.
  // Remember to apply any change here to that copy too.
  /**
   * Verifies once per service instance that pgvector is installed and explicitly activated.
   *
   * <p>Both checks are round trips, and the capability one reads {@code pg_available_extensions},
   * a view that stats and parses the extension control files on disk. They used to run on every
   * store call, so a batch of one hundred events paid them one hundred times. Only the successful
   * outcome is remembered: a failure is re-checked, so a caller that runs before activation starts
   * working as soon as the administrator activates.</p>
   */
  private void requireActive() {
    if (activationVerified) {
      return;
    }
    VectorCapability c = capability.inspect();
    if (c.getState() != VectorCapabilityState.ACTIVE || !VectorActivationService.isActivated(cp)) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,
          "pgvector is not explicitly activated for this database.");
    }
    activationVerified = true;
  }
  private CollectionInfo collection(String namespace) {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT dimensions, client_scoped, organization_scoped"
            + " FROM etarc_vector.etarc_vector_collection"
            + " WHERE namespace = ? AND active = true")) {
      ps.setString(1, namespace);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new VectorException(VectorErrorCode.VECTOR_COLLECTION_NOT_FOUND,
              "Vector collection was not found.");
        }
        return new CollectionInfo(rs.getInt(1), rs.getBoolean(2), rs.getBoolean(3));
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,
          "Could not read vector collection.", e);
    }
  }
  private static void validateScope(CollectionInfo c, String client, java.util.List<String> organizationIds) {
    if ((c.client && client == null) || (c.org && organizationIds.isEmpty())) {
      throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA, "Required vector tenant scope is missing.");
    }
  }

  private static void validateScope(CollectionInfo c,String client,String org){if((c.client&&client==null)||(c.org&&org==null))throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA,"Required vector tenant scope is missing.");}
  private static String scopeValue(String scope) { return scope == null ? "" : scope; }
  private static void validateMetadata(String metadata){if(metadata==null||!metadata.trim().startsWith("{")||!metadata.trim().endsWith("}"))throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA,"Vector metadata must be a JSON object.");}
  private static String literal(double[] v){StringBuilder b=new StringBuilder("[");for(int i=0;i<v.length;i++){if(!Double.isFinite(v[i]))throw new VectorException(VectorErrorCode.VECTOR_DIMENSION_MISMATCH,"Vector values must be finite.");if(i>0)b.append(',');b.append(v[i]);}return b.append(']').toString();}
  private static class CollectionInfo { final int dimensions; final boolean client,org; CollectionInfo(int d,boolean c,boolean o){dimensions=d;client=c;org=o;} }
}
