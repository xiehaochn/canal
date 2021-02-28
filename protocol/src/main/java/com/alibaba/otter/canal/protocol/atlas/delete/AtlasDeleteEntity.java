package com.alibaba.otter.canal.protocol.atlas.delete;

import com.alibaba.otter.canal.protocol.atlas.base.AtlasBaseEntity;

import java.util.HashMap;

public class AtlasDeleteEntity extends AtlasBaseEntity {
  private HashMap<String, String> uniqueAttributes;

  public HashMap<String, String> getUniqueAttributes() {
    return uniqueAttributes;
  }

  public void setUniqueAttributes(HashMap<String, String> uniqueAttributes) {
    this.uniqueAttributes = uniqueAttributes;
  }
}
