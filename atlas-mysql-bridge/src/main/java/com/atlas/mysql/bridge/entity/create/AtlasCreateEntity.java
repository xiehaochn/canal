package com.atlas.mysql.bridge.entity.create;

import com.atlas.mysql.bridge.entity.base.AtlasBaseEntity;

import java.util.HashMap;

public class AtlasCreateEntity extends AtlasBaseEntity {
  private HashMap<String, Object> attributes;
  private String guid;
  private boolean proxy = false;

  private int version;

  public static final String RDBMS_INSTANCE = "rdbms_instance";
  public static final String RDBMS_DB = "rdbms_db";
  public static final String RDBMS_TABLE = "rdbms_table";
  public static final String RDBMS_COLUMN = "rdbms_column";
  public static final String RDBMS_INDEX = "rdbms_index";
  public static final String RDBMS_FOREIGN_KEY = "rdbms_foreign_key";

  public HashMap<String, Object> getAttributes() {
    return attributes;
  }

  public void setAttributes(HashMap<String, Object> attributes) {
    this.attributes = attributes;
  }

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  public boolean isProxy() {
    return proxy;
  }

  public void setProxy(boolean proxy) {
    this.proxy = proxy;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "AtlasCreateEntity{"
        + "attributes="
        + attributes
        + ", guid='"
        + guid
        + '\''
        + ", proxy="
        + proxy
        + ", typeName='"
        + typeName
        + '\''
        + ", version="
        + version
        + '}';
  }
}
