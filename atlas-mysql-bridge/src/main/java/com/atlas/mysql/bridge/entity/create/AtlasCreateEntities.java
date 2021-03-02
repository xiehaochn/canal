package com.atlas.mysql.bridge.entity.create;

import com.atlas.mysql.bridge.entity.base.AtlasBaseEntity;

import java.util.List;
import java.util.Map;

public class AtlasCreateEntities {
  private List<AtlasBaseEntity> entities;
  private Map<String, AtlasCreateEntity> referredEntities;

  public List<AtlasBaseEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<AtlasBaseEntity> entities) {
    this.entities = entities;
  }

  public Map<String, AtlasCreateEntity> getReferredEntities() {
    return referredEntities;
  }

  public void setReferredEntities(Map<String, AtlasCreateEntity> referredEntities) {
    this.referredEntities = referredEntities;
  }

  @Override
  public String toString() {
    return "AtlasCreateEntities{"
        + "entities="
        + entities
        + ", referredEntities="
        + referredEntities
        + '}';
  }
}
