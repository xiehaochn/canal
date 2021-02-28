package com.alibaba.otter.canal.protocol.atlas.create;

import com.alibaba.otter.canal.protocol.atlas.base.AtlasBaseMessage;

public class AtlasCreateMessage extends AtlasBaseMessage {
  private AtlasCreateEntities entities;

  public AtlasCreateEntities getEntities() {
    return entities;
  }

  public void setEntities(AtlasCreateEntities entities) {
    this.entities = entities;
  }
}
