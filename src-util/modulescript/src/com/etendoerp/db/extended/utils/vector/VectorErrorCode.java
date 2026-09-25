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

/**
 * Stable error codes exposed by the generic vector API.
 *
 * <p>SYNC: copy of {@code com.etendoerp.db.extended.vector.VectorErrorCode} for the
 * {@code GenerateVectorSourceTriggers} post-update script: it runs inside update.database before
 * the runtime sources are compiled, so it can only use classes shipped under {@code src-util}.
 * Remember to apply any change here to the runtime class too.</p>
 */
public enum VectorErrorCode {
  PGVECTOR_NOT_ENABLED, VECTOR_COLLECTION_NOT_FOUND, VECTOR_DIMENSION_MISMATCH,
  VECTOR_INVALID_METADATA, VECTOR_EXTENSION_CONFLICT, VECTOR_INDEX_OPERATION_FAILED,
  VECTOR_OUTBOX_OPERATION_FAILED, VECTOR_EMBEDDING_OPERATION_FAILED,
  VECTOR_SEARCH_OPERATION_FAILED
}
