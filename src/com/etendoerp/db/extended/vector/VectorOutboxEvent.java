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
package com.etendoerp.db.extended.vector;

import java.util.Objects;

/** Immutable event emitted by a configured generic vector source. */
public final class VectorOutboxEvent {
  private final String id;
  private final String sourceId;
  private final long configVersion;
  private final String namespace;
  private final String recordId;
  private final String eventType;
  private final String changedColumnId;
  private final String clientId;
  private final String organizationId;

  VectorOutboxEvent(String id, String sourceId, long configVersion, String namespace, String recordId, String eventType,
      String changedColumnId, String clientId, String organizationId) {
    this.id = Objects.requireNonNull(id, "id");
    this.sourceId = Objects.requireNonNull(sourceId, "sourceId");
    this.configVersion = configVersion;
    this.namespace = Objects.requireNonNull(namespace, "namespace");
    this.recordId = Objects.requireNonNull(recordId, "recordId");
    this.eventType = Objects.requireNonNull(eventType, "eventType");
    this.changedColumnId = changedColumnId;
    this.clientId = clientId;
    this.organizationId = organizationId;
  }

  public String getId() { return id; }
  public String getSourceId() { return sourceId; }
  public long getConfigVersion() { return configVersion; }
  public String getNamespace() { return namespace; }
  public String getRecordId() { return recordId; }
  public String getEventType() { return eventType; }
  public String getChangedColumnId() { return changedColumnId; }
  public String getClientId() { return clientId; }
  public String getOrganizationId() { return organizationId; }
}
