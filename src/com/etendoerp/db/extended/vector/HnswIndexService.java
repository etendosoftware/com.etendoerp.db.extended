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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/** Explicit HNSW index lifecycle; exact search remains available without an index. */
public class HnswIndexService {
  private static final Logger log = LogManager.getLogger();
  private static final String COLLECTION_NOT_FOUND = "Vector collection was not found.";

  private final ConnectionProvider cp; private final VectorCapabilityService capability;
  public HnswIndexService(ConnectionProvider cp) { this.cp = cp; capability = new VectorCapabilityService(cp); }
  public void create(String namespace, DistanceMetric metric) {
    VectorCapability c = capability.inspect(); if (c.getState() != VectorCapabilityState.ACTIVE || !VectorActivationService.isActivated(cp)) throw VectorActivationService.disabled(c);
    String index = "etarc_vec_hnsw_" + collectionId(namespace);
    String template = "SELECT format('CREATE INDEX IF NOT EXISTS %I ON etarc_vector.etarc_vector_record USING hnsw (embedding " + metric.getOperatorClass() + ") WHERE namespace = %L', ?, ?)";
    try (PreparedStatement format = cp.getPreparedStatement(template)) { format.setString(1, index); format.setString(2, namespace); try (java.sql.ResultSet rs = format.executeQuery()) { if (!rs.next()) throw new java.sql.SQLException("Could not create HNSW statement"); try (PreparedStatement create = cp.getPreparedStatement(rs.getString(1))) { create.executeUpdate(); } } updateStatus(namespace, "READY"); }
    catch (Exception e) { updateStatus(namespace, "FAILED"); throw new VectorException(VectorErrorCode.VECTOR_INDEX_OPERATION_FAILED, "Could not create the HNSW vector index.", e); }
  }
  /**
   * Reads the index state stored for a collection.
   *
   * @param namespace
   *     the collection to inspect
   * @return the stored index status
   */
  public String status(String namespace) {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT index_status FROM etarc_vector.etarc_vector_collection WHERE namespace = ?")) {
      ps.setString(1, namespace);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new VectorException(VectorErrorCode.VECTOR_COLLECTION_NOT_FOUND,
              COLLECTION_NOT_FOUND);
        }
        return rs.getString(1);
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_INDEX_OPERATION_FAILED,
          "Could not inspect the HNSW index.", e);
    }
  }
  private long collectionId(String namespace) {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT id FROM etarc_vector.etarc_vector_collection WHERE namespace = ?")) {
      ps.setString(1, namespace);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new VectorException(VectorErrorCode.VECTOR_COLLECTION_NOT_FOUND,
              COLLECTION_NOT_FOUND);
        }
        return rs.getLong(1);
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_INDEX_OPERATION_FAILED,
          "Could not resolve vector collection identity.", e);
    }
  }
  private void updateStatus(String namespace, String status) {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "UPDATE etarc_vector.etarc_vector_collection SET index_status = ? WHERE namespace = ?")) {
      ps.setString(1, status);
      ps.setString(2, namespace);
      ps.executeUpdate();
    } catch (Exception e) {
      // The caller is already reporting the failure this was recording, so this must not replace
      // it with one about the recording. It is logged instead, because a status left behind makes
      // the collection describe an index it does not have.
      log.warn("Could not set the index status of vector collection {} to {}.", namespace, status,
          e);
    }
  }
}
