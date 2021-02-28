package com.alibaba.otter.canal.protocol.atlas.base;

public class AtlasBaseMessage {
  private HookNotificationType type;
  private String user;

  public HookNotificationType getType() {
    return type;
  }

  public void setType(HookNotificationType type) {
    this.type = type;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }
}
