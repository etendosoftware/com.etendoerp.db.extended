/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing rights
 * and limitations under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.modulescript;

import org.openbravo.modulescript.ModuleScript;

import com.etendoerp.db.extended.vector.VectorTriggerService;

/**
 * Materializes PostgreSQL outbox triggers for the enabled generic vector sources.
 *
 * <p>The Application Dictionary is the source of truth: a source selects an AD table and the
 * selected child rows determine the columns that enqueue an UPDATE event. The generated trigger
 * never creates embeddings or calls external services; it only inserts a PENDING event in
 * {@code ETARC_VECTOR_OUTBOX}. This script runs after dictionary data is installed so it can see
 * the complete source configuration.</p>
 *
 * <p>It is a plain {@link ModuleScript} rather than a post-update one on purpose. DBSM runs these
 * before it stamps the structure checksum -- {@code executeModuleScripts} precedes
 * {@code updateCRC} in {@code DBUpdater} -- so the triggers this creates are accepted by the same
 * update that created them, with nothing left to re-stamp afterwards. A post-update script runs
 * after that stamp, and would also tie this module to a core carrying the post-update framework.</p>
 *
 * <p>The generation itself lives in {@link VectorTriggerService}, in the runtime source tree,
 * because the Search Source window offers the same action for the sources an administrator
 * selects and a module script cannot be called from a window. Leaving the logic here and copying
 * it there would give the module two sets of trigger names to keep in agreement with the ones the
 * build validation excludes, which is one set too many.</p>
 */
public class GenerateVectorSourceTriggers extends ModuleScript {

  @Override
  public void execute() {
    try {
      new VectorTriggerService(getConnectionProvider()).deployAll();
    } catch (Exception e) {
      handleError(e);
    }
  }
}
