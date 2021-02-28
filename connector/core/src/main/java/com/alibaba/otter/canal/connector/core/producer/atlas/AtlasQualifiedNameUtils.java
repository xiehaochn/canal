package com.alibaba.otter.canal.connector.core.producer.atlas;

public class AtlasQualifiedNameUtils {
  protected static String getIdxQuaName(
      String hostName, String port, String dbName, String tabName, String name) {
    StringBuilder idxQuaName = new StringBuilder();
    idxQuaName
        .append("<")
        .append(hostName)
        .append(">.")
        .append(dbName)
        .append(".")
        .append(tabName)
        .append(".<index>")
        .append(name)
        .append("@")
        .append(port);
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

  protected static String getColQuaName(
      String hostName, String port, String dbName, String tabName, String colName) {
    StringBuilder colQuaName = new StringBuilder();
    colQuaName
        .append("<")
        .append(hostName)
        .append(">.")
        .append(dbName)
        .append(".")
        .append(tabName)
        .append(".")
        .append(colName)
        .append("@")
        .append(port);
    return colQuaName.toString();
  }

  protected static String getTabQueName(
      String hostName, String port, String dbName, String tabName) {
    StringBuilder tabQuaName = new StringBuilder();
    tabQuaName
        .append("<")
        .append(hostName)
        .append(">.")
        .append(dbName)
        .append(".")
        .append(tabName)
        .append("@")
        .append(port);
    return tabQuaName.toString();
  }
}
