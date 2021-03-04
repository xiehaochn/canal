package com.atlas.mysql.bridge.utils;

import com.atlas.mysql.bridge.entity.base.AtlasBaseEntity;
import com.atlas.mysql.bridge.entity.create.AtlasCreateEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class AtlasAttributesQueryUtils {

  public static List<AtlasCreateEntity> getAttFromSchema(
      Connection connection,
      String sql,
      TabInfo tabInfo,
      String entityType,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap) {
    List<AtlasCreateEntity> entities = new ArrayList<>();
    PreparedStatement preSta = null;
    try {
      preSta = connection.prepareStatement(sql);
      preSta.setString(1, tabInfo.getDbName());
      preSta.setString(2, tabInfo.getTabName());
      ResultSet resultSet = preSta.executeQuery();
      while (resultSet.next()) {
        switch (entityType) {
          case AtlasCreateEntity.RDBMS_COLUMN:
            entities.add(getColEnt(resultSet, tabInfo));
            break;
          case AtlasCreateEntity.RDBMS_INDEX:
            AtlasCreateEntity idxEntity = getIdxEnt(resultSet, tabInfo, idxColMap, colIdxMap);
            if (idxEntity != null) {
              entities.add(idxEntity);
            }
            break;
          case AtlasCreateEntity.RDBMS_TABLE:
            entities.add(getTabEnt(resultSet, tabInfo));
            break;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (preSta != null) {
          preSta.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return entities;
  }

  private static AtlasCreateEntity getTabEnt(ResultSet resultSet, TabInfo tabInfo)
      throws SQLException {
    HashMap<String, Object> tableAttributes = new HashMap<>();
    //      String tableRows = resultSet.getString("table_rows");
    //      String tableAvgRowLen = resultSet.getString("avg_row_length");
    //      String tableUpdateTime = resultSet.getString("update_time");
    tableAttributes.put("name", resultSet.getString("table_name"));
    tableAttributes.put("description", resultSet.getString("table_comment"));
    tableAttributes.put("createTime", Long.toString(resultSet.getDate("create_time").getTime()));
    tableAttributes.put("comment", resultSet.getString("table_comment"));
    tableAttributes.put("type", resultSet.getString("table_type"));
    tableAttributes.put("qualifiedName", AtlasQualifiedNameUtils.getTabQueName(tabInfo));
    AtlasCreateEntity tableEntity = new AtlasCreateEntity();
    tableEntity.setGuid(AtlasBaseEntity.nextInternalId());
    tableEntity.setTypeName(AtlasCreateEntity.RDBMS_TABLE);
    tableEntity.setVersion(0);
    tableEntity.setAttributes(tableAttributes);
    return tableEntity;
  }

  private static AtlasCreateEntity getIdxEnt(
      ResultSet resultSet,
      TabInfo tabInfo,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap)
      throws SQLException {
    String indexName = resultSet.getString("index_name");
    String columnName = resultSet.getString("column_name");
    if (colIdxMap.containsKey(columnName)) {
      colIdxMap.get(columnName).add(indexName);
    } else {
      HashSet<String> idxSet = new HashSet<>();
      idxSet.add(indexName);
      colIdxMap.put(columnName, idxSet);
    }
    if (idxColMap.containsKey(indexName)) {
      idxColMap.get(indexName).add(columnName);
      return null;
    } else {
      HashSet<String> colSet = new HashSet<>();
      colSet.add(columnName);
      idxColMap.put(indexName, colSet);
    }
    HashMap<String, Object> idxAttributes = new HashMap<>();
    idxAttributes.put("name", resultSet.getString("index_name"));
    idxAttributes.put("description", resultSet.getString("index_comment"));
    idxAttributes.put("index_type", resultSet.getString("index_type"));
    idxAttributes.put("isUnique", resultSet.getInt("non_unique") == 0);
    idxAttributes.put("comment", resultSet.getString("index_comment"));
    idxAttributes.put(
        "qualifiedName",
        AtlasQualifiedNameUtils.getIdxQuaName(tabInfo, (String) idxAttributes.get("name")));
    AtlasCreateEntity idxEntity = new AtlasCreateEntity();
    idxEntity.setGuid(AtlasBaseEntity.nextInternalId());
    idxEntity.setTypeName(AtlasCreateEntity.RDBMS_INDEX);
    idxEntity.setVersion(0);
    idxEntity.setAttributes(idxAttributes);
    return idxEntity;
  }

  private static AtlasCreateEntity getColEnt(ResultSet resultSet, TabInfo tabInfo)
      throws SQLException {
    HashMap<String, Object> colAttributes = new HashMap<>();
    colAttributes.put("name", resultSet.getString("column_name"));
    colAttributes.put("description", resultSet.getString("column_comment"));
    colAttributes.put("data_type", resultSet.getString("column_type"));
    long columnLength = resultSet.getLong("character_maximum_length");
    colAttributes.put("length", columnLength > Integer.MAX_VALUE ? -1 : (int) columnLength);
    colAttributes.put("default_value", resultSet.getString("column_default"));
    colAttributes.put("comment", resultSet.getString("column_comment"));
    colAttributes.put("isNullable", resultSet.getString("is_nullable").equalsIgnoreCase("YES"));
    colAttributes.put("isPrimaryKey", resultSet.getString("column_key").equalsIgnoreCase("PRI"));
    colAttributes.put(
        "qualifiedName",
        AtlasQualifiedNameUtils.getColQuaName(tabInfo, (String) colAttributes.get("name")));
    AtlasCreateEntity colEntity = new AtlasCreateEntity();
    colEntity.setGuid(AtlasBaseEntity.nextInternalId());
    colEntity.setTypeName(AtlasCreateEntity.RDBMS_COLUMN);
    colEntity.setVersion(0);
    colEntity.setAttributes(colAttributes);
    return colEntity;
  }

  public static class TabInfo {
    private String hostName;
    private String port;
    private String dbName;
    private String tabName;

    public TabInfo(String hostName, String port, String dbName, String tabName) {
      this.hostName = hostName;
      this.port = port;
      this.dbName = dbName;
      this.tabName = tabName;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public String getPort() {
      return port;
    }

    public void setPort(String port) {
      this.port = port;
    }

    public String getDbName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public String getTabName() {
      return tabName;
    }

    public void setTabName(String tabName) {
      this.tabName = tabName;
    }
  }
}
