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

import org.openbravo.modulescript.PostUpdateModuleScript;

import com.etendoerp.db.extended.vector.VectorProvisioningService;
import com.etendoerp.db.extended.vector.VectorStoreService;

/**
 * Materializes PostgreSQL outbox triggers for the enabled generic vector sources.
 *
 * <p>The Application Dictionary is the source of truth: a source selects an AD table and the
 * selected child rows determine the columns that enqueue an UPDATE event. The generated trigger
 * never creates embeddings or calls external services; it only inserts a PENDING event in
 * {@code ETARC_VECTOR_OUTBOX}. This script runs after dictionary data is installed so it can see
 * the complete source configuration.</p>
 *
 * <p>It is a {@link PostUpdateModuleScript} because it has to read the dictionary as the update
 * leaves it. A plain module script runs earlier than its name suggests: in {@code DBUpdater} the
 * order is {@code executeModuleScripts} and only then {@code Platform.alterData}, which is where
 * the sourcedata is applied. One would therefore generate the triggers of a source from the
 * configuration the update was about to replace, which is wrong precisely for the sources a module
 * ships -- the ones whose rows arrive in that import.</p>
 *
 * <p>It provisions the whole capability, not only the triggers: the extension and its storage,
 * the collection each source needs, and then the triggers. All of it is DDL, and this is the one
 * moment at which DDL is free -- the update that performs it is also the one that accepts the
 * structure it leaves behind. The window no longer does any of this; it reports what a source
 * still needs, and the change is applied by the next update.database.</p>
 *
 * <p>The work itself lives in {@link VectorProvisioningService}, in the runtime source tree, so
 * the window can report from the same definitions this provisions from.</p>
 */
public class GenerateVectorSourceTriggers extends PostUpdateModuleScript {

  @Override
  public void execute() {
    try {
      new VectorProvisioningService(getConnectionProvider(),
          new VectorStoreService(getConnectionProvider())).provision();
    } catch (Exception e) {
      handleError(e);
    }
  }
}
