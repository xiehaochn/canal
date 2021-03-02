package com.atlas.mysql.bridge.utils;

public class AtlasQualifiedNameUtils {
  protected static String getIdxQuaName(AtlasAttributesQueryUtils.TabInfo tabInfo, String name) {
    StringBuilder idxQuaName = new StringBuilder();
    idxQuaName
        .append("<")
        .append(tabInfo.getHostName())
        .append(">.")
        .append(tabInfo.getDbName())
        .append(".")
        .append(tabInfo.getTabName())
        .append(".<index>")
        .append(name)
        .append("@")
        .append(tabInfo.getPort());
    return idxQuaName.toString();
  }

  protected static String getInstanceQuaName(String hostName, String port) {
    return "<" + hostName + ">@" + port;
  }

  protected static String getDbQuaName(String hostName, String port, String dbName) {
    StringBuilder dbQuaName = new StringBuilder();
    dbQuaName.append("<").append(hostName).append(">.").append(dbName).append("@").append(port);
    return dbQuaName.toString();
  }

  protected static String getColQuaName(AtlasAttributesQueryUtils.TabInfo tabInfo, String colName) {
    StringBuilder colQuaName = new StringBuilder();
    colQuaName
        .append("<")
        .append(tabInfo.getHostName())
        .append(">.")
        .append(tabInfo.getDbName())
        .append(".")
        .append(tabInfo.getTabName())
        .append(".")
        .append(colName)
        .append("@")
        .append(tabInfo.getPort());
    return colQuaName.toString();
  }

  protected static String getTabQueName(AtlasAttributesQueryUtils.TabInfo tabInfo) {
    StringBuilder tabQuaName = new StringBuilder();
    tabQuaName
        .append("<")
        .append(tabInfo.getHostName())
        .append(">.")
        .append(tabInfo.getDbName())
        .append(".")
        .append(tabInfo.getTabName())
        .append("@")
        .append(tabInfo.getPort());
    return tabQuaName.toString();
  }
}
