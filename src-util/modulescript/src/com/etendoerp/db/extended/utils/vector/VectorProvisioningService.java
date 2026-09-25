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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

import com.etendoerp.db.extended.utils.vector.VectorSourceReadiness.Candidate;
import com.etendoerp.db.extended.utils.vector.VectorSourceReadiness.Verdict;

/**
 * Brings the database in line with the configured search sources: the extension and its storage,
 * a collection per source that needs one, and the change capture triggers.
 *
 * <p>This runs from the post-update module script, inside {@code update.database}, and nowhere
 * else. Everything it does is DDL, and DDL performed while the application is running moves the
 * structure checksum -- which the next update refuses to run over, and which no runtime caller can
 * accept on an administrator's behalf without also accepting whatever else had been changed.
 * Doing it here means the update that makes the change is the one that accepts it.</p>
 *
 * <p>Configuring a source is therefore a two step operation: save it, then run update.database.
 * The scheduled processes take it from there.</p>
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.VectorProvisioningService} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 *
 * <p>Unlike the runtime class it does not take a {@code VectorStore}: the store and its search and
 * upsert types are runtime-only, and provisioning needs a single operation from it, so
 * {@link #createCollection} carries that one operation instead.</p>
 */
public class VectorProvisioningService {

  private static final Logger log = LogManager.getLogger();

  private final ConnectionProvider cp;
  private final VectorCapabilityService capability;
  private final java.util.Properties systemProperties;

  /**
   * Creates a service that provisions what the configured sources need.
   *
   * @param cp
   *     connection the provisioning statements are issued with
   * @param systemProperties
   *     the Openbravo properties, so the extension can be created as the system user when the
   *     application user may not
   */
  public VectorProvisioningService(ConnectionProvider cp, java.util.Properties systemProperties) {
    this.cp = cp;
    this.capability = new VectorCapabilityService(cp);
    this.systemProperties = systemProperties;
  }

  /**
   * Provisions everything the configured sources need, and removes what no source needs any more.
   *
   * <p>Nothing is installed for an instance that configured no source. The capability stays off
   * until somebody asks for it, and merely having the module installed is not asking: the
   * extension is not created, and neither is any vector object. Configuring a usable source is the
   * request, and this is where it is granted.</p>
   *
   * <p>The triggers are swept either way, because a source that was deleted or turned off since
   * the previous update leaves behind triggers that nothing else would remove.</p>
   *
   * @return the number of sources left with change capture installed
   * @throws Exception
   *     if the database cannot be provisioned
   */
  public int provision() throws Exception {
    List<Candidate> candidates = VectorSourceReadiness.candidates(cp);
    if (wantsStorage(candidates)) {
      new VectorActivationService(cp, systemProperties).activate();
      createMissingCollections(candidates);
    } else {
      log.debug("No search source asks for vector storage; nothing is installed.");
    }
    return new VectorTriggerService(cp).deployAll();
  }

  /**
   * Whether any source would be usable once it had a collection.
   *
   * <p>Asked before the storage exists, so the verdict is taken against no collection: every
   * refusal other than the missing collection itself is a source that is not asking for one.</p>
   */
  private static boolean wantsStorage(List<Candidate> candidates) {
    return candidates.stream()
        .anyMatch(candidate -> VectorSourceReadiness.verdict(candidate, null).isUsable());
  }

  private void createMissingCollections(List<Candidate> candidates) throws Exception {
    for (Candidate candidate : candidates) {
      Verdict verdict = VectorSourceReadiness.verdict(candidate,
          VectorSourceReadiness.collection(cp, candidate.namespace));
      if (verdict != Verdict.COLLECTION_MISSING) {
        continue;
      }
      // Tenant scope is always on: the search context derives client and organization from the
      // session and never lets a caller supply them, so a collection that did not require them
      // would accept records no search could ever reach.
      createCollection(new VectorCollection(candidate.namespace, candidate.dimensions,
          DistanceMetric.valueOf(candidate.metric), true, true));
      log.info("Created vector collection {} for source {}.", candidate.namespace, candidate.name);
    }
  }

  /**
   * Stores a collection definition, once pgvector is installed and explicitly activated.
   *
   * <p>SYNC: copy of {@code VectorStoreService#createCollection} and its
   * {@code requireActive} check in {@code com.etendoerp.db.extended.vector}. Remember to apply any
   * change here to those runtime methods too.</p>
   */
  private void createCollection(VectorCollection collection) {
    if (capability.inspect().getState() != VectorCapabilityState.ACTIVE
        || !VectorActivationService.isActivated(cp)) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,
          "pgvector is not explicitly activated for this database.");
    }
    String sql = "INSERT INTO etarc_vector.etarc_vector_collection (namespace, dimensions, metric,"
        + " client_scoped, organization_scoped) VALUES (?, ?, ?, ?, ?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, collection.getNamespace());
      ps.setInt(2, collection.getDimensions());
      ps.setString(3, collection.getMetric().name());
      ps.setBoolean(4, collection.isClientScoped());
      ps.setBoolean(5, collection.isOrganizationScoped());
      ps.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.PGVECTOR_NOT_ENABLED,
          "Could not create vector collection.", e);
    }
  }
}
