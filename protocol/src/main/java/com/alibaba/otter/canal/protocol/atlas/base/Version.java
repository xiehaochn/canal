package com.alibaba.otter.canal.protocol.atlas.base;

import java.util.List;

public class Version {
  private String version;
  private List<Integer> versionParts;

  public Version(String version, List<Integer> versionParts) {
    this.version = version;
    this.versionParts = versionParts;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public List<Integer> getVersionParts() {
    return versionParts;
  }

  public void setVersionParts(List<Integer> versionParts) {
    this.versionParts = versionParts;
  }
}
