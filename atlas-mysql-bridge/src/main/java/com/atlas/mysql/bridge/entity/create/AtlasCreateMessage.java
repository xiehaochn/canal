package com.atlas.mysql.bridge.entity.create;

import com.atlas.mysql.bridge.entity.base.AtlasBaseMessage;

public class AtlasCreateMessage extends AtlasBaseMessage {
  private AtlasCreateEntities entities;

  public AtlasCreateEntities getEntities() {
    return entities;
  }

  public void setEntities(AtlasCreateEntities entities) {
    this.entities = entities;
  }
}
