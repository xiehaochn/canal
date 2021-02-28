package com.alibaba.otter.canal.protocol.atlas.delete;

import com.alibaba.otter.canal.protocol.atlas.base.AtlasBaseMessage;

import java.util.List;

public class AtlasDeleteMessage extends AtlasBaseMessage {
  private List<AtlasDeleteEntity> entities;

  public List<AtlasDeleteEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<AtlasDeleteEntity> entities) {
    this.entities = entities;
  }
}
