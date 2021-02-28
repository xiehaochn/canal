package com.alibaba.otter.canal.protocol.atlas.base;

import java.util.concurrent.atomic.AtomicLong;

public class AtlasBaseEntity {
  protected String typeName;

  private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public static String nextInternalId() {
    return "-" + s_nextId.getAndIncrement();
  }
}
