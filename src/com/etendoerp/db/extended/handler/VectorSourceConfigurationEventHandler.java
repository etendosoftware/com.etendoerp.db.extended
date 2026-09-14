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
package com.etendoerp.db.extended.handler;

import java.util.Arrays;
import java.util.List;

import javax.enterprise.event.Observes;

import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.base.model.Property;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.client.kernel.event.EntityNewEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEventObserver;
import org.openbravo.client.kernel.event.EntityUpdateEvent;

import com.etendoerp.db.extended.data.VectorSource;
import com.etendoerp.db.extended.data.VectorSourceColumn;

/**
 * Moves a vector source to a new configuration version when what it indexes changes.
 *
 * <p>The version is the fence that keeps queued events honest: every event carries the version it
 * was created under, and the dispatcher retires those whose source has moved on rather than
 * indexing content built under rules that no longer apply. Until now nothing ever moved it, so the
 * fence could not fire on its own and stale events were delivered as if nothing had changed.</p>
 *
 * <p>Only changes that alter what gets embedded or where it is stored count. Renaming a source or
 * editing its description leaves queued events perfectly valid, and invalidating them would throw
 * away work for a cosmetic edit.</p>
 *
 * <p>Bumping the version does not start a reindex. It marks that whatever is already queued no
 * longer applies; rebuilding what was indexed before is an explicit decision, taken through a
 * reindex request.</p>
 */
public class VectorSourceConfigurationEventHandler extends EntityPersistenceEventObserver {

  private static final Entity[] entities = {
      ModelProvider.getInstance().getEntity(VectorSource.ENTITY_NAME),
      ModelProvider.getInstance().getEntity(VectorSourceColumn.ENTITY_NAME) };

  /** Properties of the source that change what is embedded or where it lands. */
  private static final List<String> VERSIONED_PROPERTIES = Arrays.asList(
      VectorSource.PROPERTY_TABLE,
      VectorSource.PROPERTY_NAMESPACE,
      VectorSource.PROPERTY_ETARCVECTOREMBEDPROVIDER,
      VectorSource.PROPERTY_DISTANCEMETRIC,
      VectorSource.PROPERTY_FILTERCOLUMN,
      VectorSource.PROPERTY_FILTERVALUE);

  @Override
  protected Entity[] getObservedEntities() {
    return entities;
  }

  public void onUpdate(@Observes EntityUpdateEvent event) {
    if (!isValidEvent(event)) {
      return;
    }
    if (event.getTargetInstance() instanceof VectorSource) {
      VectorSource source = (VectorSource) event.getTargetInstance();
      if (changedAny(event, source)) {
        bump(event, source);
      }
    } else if (event.getTargetInstance() instanceof VectorSourceColumn) {
      bump(((VectorSourceColumn) event.getTargetInstance()).getEtarcVectorSource());
    }
  }

  public void onNew(@Observes EntityNewEvent event) {
    if (isValidEvent(event) && event.getTargetInstance() instanceof VectorSourceColumn) {
      bump(((VectorSourceColumn) event.getTargetInstance()).getEtarcVectorSource());
    }
  }

  public void onDelete(@Observes EntityDeleteEvent event) {
    if (isValidEvent(event) && event.getTargetInstance() instanceof VectorSourceColumn) {
      bump(((VectorSourceColumn) event.getTargetInstance()).getEtarcVectorSource());
    }
  }

  private boolean changedAny(EntityUpdateEvent event, VectorSource source) {
    Entity entity = ModelProvider.getInstance().getEntity(VectorSource.ENTITY_NAME);
    for (String propertyName : VERSIONED_PROPERTIES) {
      Property property = entity.getProperty(propertyName);
      Object previous = event.getPreviousState(property);
      Object current = event.getCurrentState(property);
      if (previous == null ? current != null : !previous.equals(current)) {
        return true;
      }
    }
    return false;
  }

  /** Bumps the version of the source being saved, through the event so the change travels with it. */
  private void bump(EntityUpdateEvent event, VectorSource source) {
    Property property = ModelProvider.getInstance()
        .getEntity(VectorSource.ENTITY_NAME)
        .getProperty(VectorSource.PROPERTY_CONFIGVERSION);
    event.setCurrentState(property, next(source.getConfigVersion()));
  }

  /** Bumps the version of a source that is not the entity being saved. */
  private void bump(VectorSource source) {
    if (source != null) {
      source.setConfigVersion(next(source.getConfigVersion()));
    }
  }

  private static Long next(Long current) {
    return current == null ? 1L : current + 1L;
  }
}
