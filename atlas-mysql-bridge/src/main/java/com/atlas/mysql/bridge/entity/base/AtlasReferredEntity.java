package com.atlas.mysql.bridge.entity.base;

public class AtlasReferredEntity {
  private String guid;
  private String typeName;

  public AtlasReferredEntity(String guid, String typeName) {
    this.guid = guid;
    this.typeName = typeName;
  }

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }
}
