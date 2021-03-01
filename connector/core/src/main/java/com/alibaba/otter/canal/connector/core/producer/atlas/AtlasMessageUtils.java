package com.alibaba.otter.canal.connector.core.producer.atlas;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.atlas.base.*;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateEntities;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateEntity;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateMessage;
import com.alibaba.otter.canal.protocol.atlas.delete.AtlasDeleteEntity;
import com.alibaba.otter.canal.protocol.atlas.delete.AtlasDeleteMessage;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AtlasMessageUtils {
  private AuthenticationInfo mysqlAuthInfo;

  private String hostName;

  private String port;

  private static final String INFORMATION_SCHEMA = "information_schema";

  private static final String SELECT_TABLE_INFO_SQL =
      "select table_schema,table_name,table_type,table_rows,avg_row_length,create_time,update_time,table_comment from information_schema.tables where table_schema=? and table_name=?";

  private static final String SELECT_COLUMNS_INFO_SQL =
      "select table_schema,table_name,column_name,ordinal_position,column_default,is_nullable,data_type,column_key,character_maximum_length,column_type,column_comment from information_schema.columns where table_schema=? and table_name=?";

  private static final String SELECT_INDEX_INFO_SQL =
      "select index_name,non_unique,index_type,index_comment,column_name from information_schema.statistics where table_schema=? and table_name=?";

  public AtlasMessageUtils(AuthenticationInfo mysqlAuthInfo) {
    this.mysqlAuthInfo = mysqlAuthInfo;
    this.hostName = mysqlAuthInfo.getAddress().getHostString();
    this.port = Integer.toString(mysqlAuthInfo.getAddress().getPort());
  }

  public List<AtlasBaseHookMessage> convertToAtlasEntityMessage(FlatMessage flatMessage) {
    switch (flatMessage.getType()) {
      case "CREATE":
        return refreshTableMassage(
            flatMessage.getDatabase().toLowerCase(),
            flatMessage.getTable().toLowerCase(),
            HookNotificationType.ENTITY_CREATE_V2);
      case "RENAME":
        return renameTabMessage(parseTabName(flatMessage), flatMessage);
      case "ALTER":
        return refreshTableMassage(
            flatMessage.getDatabase().toLowerCase(),
            flatMessage.getTable().toLowerCase(),
            HookNotificationType.ENTITY_FULL_UPDATE_V2);
      case "CINDEX":
        return createIndexMessage(
            flatMessage.getDatabase().toLowerCase(), flatMessage.getTable().toLowerCase());
      case "DINDEX":
        return deleteIndexMessage(
            flatMessage.getDatabase().toLowerCase(),
            flatMessage.getTable().toLowerCase(),
            flatMessage.getSql());
      case "ERASE":
        return eraseTableMassage(
            flatMessage.getDatabase().toLowerCase(), flatMessage.getTable().toLowerCase());
      default:
        return null;
    }
  }

  private List<AtlasBaseHookMessage> deleteIndexMessage(String db, String table, String sql) {
    List<AtlasBaseHookMessage> dIdxMessages = new ArrayList<>();
    AtlasBaseHookMessage atlasBaseHookMessage = new AtlasBaseHookMessage();
    setCommonProperties(atlasBaseHookMessage);
    AtlasDeleteMessage deleteIdxMessage = new AtlasDeleteMessage();
    deleteIdxMessage.setType(HookNotificationType.ENTITY_DELETE_V2);
    deleteIdxMessage.setUser(mysqlAuthInfo.getUsername());
    String idxName = "";
    String pattern = "index\\s.*\\son";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(sql.toLowerCase());
    while (m.find()) {
      String tmp = m.group();
      String[] splitString = tmp.replaceAll("'", "").split("\\s");
      idxName = splitString[1];
    }
    AtlasDeleteEntity atlasDeleteEntity = new AtlasDeleteEntity();
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put(
        "qualifiedName", AtlasQualifiedNameUtils.getIdxQuaName(hostName, port, db, table, idxName));
    atlasDeleteEntity.setUniqueAttributes(hashMap);
    atlasDeleteEntity.setTypeName(AtlasCreateEntity.RDBMS_INDEX);
    deleteIdxMessage.setEntities(Collections.singletonList(atlasDeleteEntity));
    atlasBaseHookMessage.setMessage(deleteIdxMessage);
    dIdxMessages.add(atlasBaseHookMessage);
    dIdxMessages.addAll(refreshTableMassage(db, table, HookNotificationType.ENTITY_FULL_UPDATE_V2));
    return dIdxMessages;
  }

  private List<AtlasBaseHookMessage> createIndexMessage(String db, String table) {
    List<AtlasBaseHookMessage> cIdxMessages = new ArrayList<>();
    List<AtlasBaseEntity> indexEntities = new ArrayList<>();
    Map<String, AtlasCreateEntity> referredEntities = new HashMap<>();
    AtlasBaseHookMessage fullMessages =
        refreshTableMassage(db, table, HookNotificationType.ENTITY_FULL_UPDATE_V2).get(0);
    for (AtlasBaseEntity entity :
        ((AtlasCreateMessage) fullMessages.getMessage()).getEntities().getEntities()) {
      referredEntities.put(((AtlasCreateEntity) entity).getGuid(), (AtlasCreateEntity) entity);
    }
    for (AtlasBaseEntity entity :
        ((AtlasCreateMessage) fullMessages.getMessage())
            .getEntities()
            .getReferredEntities()
            .values()) {
      if (entity.getTypeName().equals(AtlasCreateEntity.RDBMS_INDEX)) {
        indexEntities.add(entity);
      } else {
        referredEntities.put(((AtlasCreateEntity) entity).getGuid(), (AtlasCreateEntity) entity);
      }
    }
    AtlasBaseHookMessage cIdxHookMessage = new AtlasBaseHookMessage();
    setCommonProperties(cIdxHookMessage);
    AtlasCreateMessage message = new AtlasCreateMessage();
    message.setType(HookNotificationType.ENTITY_CREATE_V2);
    message.setUser(mysqlAuthInfo.getUsername());
    AtlasCreateEntities atlasCreateEntities = new AtlasCreateEntities();
    atlasCreateEntities.setEntities(indexEntities);
    atlasCreateEntities.setReferredEntities(referredEntities);
    message.setEntities(atlasCreateEntities);
    cIdxHookMessage.setMessage(message);
    cIdxMessages.add(cIdxHookMessage);
    //    cIdxMessages.add(fullMessages);
    return cIdxMessages;
  }

  private List<AtlasBaseHookMessage> refreshTableMassage(
      String dbName, String tabName, HookNotificationType messageType) {
    AtlasBaseHookMessage cTableMessage = new AtlasBaseHookMessage();
    setCommonProperties(cTableMessage);
    AtlasCreateMessage message = new AtlasCreateMessage();
    message.setType(messageType);
    message.setUser(mysqlAuthInfo.getUsername());
    message.setEntities(getTableAtlasEntities(dbName, tabName));
    cTableMessage.setMessage(message);
    return Collections.singletonList(cTableMessage);
  }

  private List<AtlasBaseHookMessage> renameTabMessage(
      HashMap<String, String> tabName, FlatMessage flatMessage) {
    List<AtlasBaseHookMessage> rTabMessages = new ArrayList<>();
    for (String oldName : tabName.keySet()) {
      String newName = tabName.get(oldName);
      String oldTabName;
      String oldDbName;
      String newTabName;
      String newDbName;
      if (oldName.contains(".")) {
        String[] tmp = oldName.split(".");
        oldDbName = tmp[0];
        oldTabName = tmp[1];
      } else {
        oldDbName = flatMessage.getDatabase().toLowerCase();
        oldTabName = oldName;
      }
      if (newName.contains(".")) {
        String[] tmp = newName.split(".");
        newDbName = tmp[0];
        newTabName = tmp[1];
      } else {
        newDbName = flatMessage.getDatabase().toLowerCase();
        newTabName = newName;
      }
      rTabMessages.addAll(eraseTableMassage(oldDbName, oldTabName));
      rTabMessages.addAll(
          refreshTableMassage(newDbName, newTabName, HookNotificationType.ENTITY_CREATE_V2));
    }
    return rTabMessages;
  }

  private List<AtlasBaseHookMessage> eraseTableMassage(String dbName, String tabName) {
    AtlasBaseHookMessage eTableMassage = new AtlasBaseHookMessage();
    setCommonProperties(eTableMassage);
    AtlasDeleteMessage message = new AtlasDeleteMessage();
    message.setType(HookNotificationType.ENTITY_DELETE_V2);
    message.setUser(mysqlAuthInfo.getUsername());
    message.setEntities(getDropTabEntities(dbName, tabName));
    eTableMassage.setMessage(message);
    return Collections.singletonList(eTableMassage);
  }

  private HashMap<String, String> parseTabName(FlatMessage flatMessage) {
    HashMap<String, String> tabName = new HashMap<>();
    String pattern = "[\\s,][\\w.]*\\s*to\\s*[\\w.]*";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(flatMessage.getSql().toLowerCase());
    while (m.find()) {
      String tmp = m.group();
      String[] tab = tmp.split("\\sto\\s");
      tabName.put(tab[0], tab[1]);
    }
    return tabName;
  }

  private List<AtlasDeleteEntity> getDropTabEntities(String dbName, String tabName) {
    AtlasDeleteEntity dropTabEnt = new AtlasDeleteEntity();
    dropTabEnt.setTypeName(AtlasCreateEntity.RDBMS_TABLE);
    HashMap<String, String> dropUniAtt = new HashMap<>();
    dropUniAtt.put(
        "qualifiedName", AtlasQualifiedNameUtils.getTabQueName(hostName, port, dbName, tabName));
    dropTabEnt.setUniqueAttributes(dropUniAtt);
    return Collections.singletonList(dropTabEnt);
  }

  private void setCommonProperties(AtlasBaseHookMessage message) {
    message.setMsgCompressionKind("NONE");
    message.setMsgCreatedBy("MySQL");
    message.setMsgCreationTime(System.currentTimeMillis());
    message.setMsgSourceIP(mysqlAuthInfo.getAddress().getHostString());
    message.setMsgSplitCount(1);
    message.setMsgSplitIdx(1);
    message.setVersion(new Version("1.0.0", Collections.singletonList(1)));
  }

  public AtlasCreateEntities getTableAtlasEntities(String dbName, String tabName) {
    Connection connection = null;
    AtlasCreateEntities atlasCreateEntities = new AtlasCreateEntities();
    try {
      connection = initConnection(mysqlAuthInfo);
      Map<String, AtlasCreateEntity> tableReferredEntities =
          getTableReferredEntities(dbName, tabName, connection);
      List<AtlasBaseEntity> tableEntities =
          getTableEntities(dbName, tabName, tableReferredEntities, connection);
      atlasCreateEntities.setEntities(tableEntities);
      atlasCreateEntities.setReferredEntities(tableReferredEntities);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return atlasCreateEntities;
  }

  private Connection initConnection(AuthenticationInfo mysqlAuthInfo) throws Exception {
    String source =
        "jdbc:mysql://"
            + mysqlAuthInfo.getAddress().getHostString()
            + ":"
            + mysqlAuthInfo.getAddress().getPort()
            + "/"
            + INFORMATION_SCHEMA
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    return DriverManager.getConnection(
        source, mysqlAuthInfo.getUsername(), mysqlAuthInfo.getPassword());
  }

  private Map<String, AtlasCreateEntity> getTableReferredEntities(
      String dbName, String tabName, Connection connection) {
    Map<String, AtlasCreateEntity> tableReferredEntities = new HashMap<>();
    HashMap<String, HashSet<String>> idxColMap = new HashMap<>();
    HashMap<String, HashSet<String>> colIdxMap = new HashMap<>();
    getColumnsEntities(tableReferredEntities, dbName, tabName, connection);
    getIndexesEntities(tableReferredEntities, dbName, tabName, connection, idxColMap, colIdxMap);
    getDbEntity(tableReferredEntities, dbName, tabName);
    setColIdxRel(tableReferredEntities, idxColMap, colIdxMap);
    return tableReferredEntities;
  }

  private void setColIdxRel(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap) {
    replaceNameWithGuid(tableReferredEntities, idxColMap, AtlasCreateEntity.RDBMS_COLUMN);
    replaceNameWithGuid(tableReferredEntities, colIdxMap, AtlasCreateEntity.RDBMS_INDEX);
    addRel(
        tableReferredEntities,
        idxColMap,
        AtlasCreateEntity.RDBMS_INDEX,
        AtlasCreateEntity.RDBMS_COLUMN,
        "columns");
    addRel(
        tableReferredEntities,
        colIdxMap,
        AtlasCreateEntity.RDBMS_COLUMN,
        AtlasCreateEntity.RDBMS_INDEX,
        "indexes");
  }

  private void addRel(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> relMap,
      String mainType,
      String attType,
      String attName) {
    for (String guid : tableReferredEntities.keySet()) {
      AtlasCreateEntity atlasCreateEntity = tableReferredEntities.get(guid);
      if (!atlasCreateEntity.getTypeName().equalsIgnoreCase(mainType)) {
        continue;
      }
      String name = (String) atlasCreateEntity.getAttributes().get("name");
      if (relMap.containsKey(name)) {
        addRelAtt(atlasCreateEntity, relMap.get(name), attName, attType);
      }
    }
  }

  private void addRelAtt(
      AtlasCreateEntity entity, HashSet<String> guidSet, String attName, String attType) {
    List<AtlasReferredEntity> atlasReferredEntities = new ArrayList<>();
    for (String guid : guidSet) {
      atlasReferredEntities.add(new AtlasReferredEntity(guid, attType));
    }
    entity.getAttributes().put(attName, atlasReferredEntities);
  }

  private void replaceNameWithGuid(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> relMap,
      String entType) {
    for (String guid : tableReferredEntities.keySet()) {
      AtlasCreateEntity atlasCreateEntity = tableReferredEntities.get(guid);
      if (!atlasCreateEntity.getTypeName().equalsIgnoreCase(entType)) {
        continue;
      }
      String name = (String) atlasCreateEntity.getAttributes().get("name");
      for (HashSet<String> values : relMap.values()) {
        if (values.contains(name)) {
          values.remove(name);
          values.add(guid);
        }
      }
    }
  }

  private void getIndexesEntities(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      String dbName,
      String tabName,
      Connection connection,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap) {
    PreparedStatement queryIndexesPreSta = null;
    try {
      queryIndexesPreSta = connection.prepareStatement(SELECT_INDEX_INFO_SQL);
      queryIndexesPreSta.setString(1, dbName);
      queryIndexesPreSta.setString(2, tabName);
      ResultSet resultSet = queryIndexesPreSta.executeQuery();
      while (resultSet.next()) {
        String indexName = resultSet.getString("index_name");
        String columnName = resultSet.getString("column_name");
        if (colIdxMap.containsKey(columnName)) {
          colIdxMap.get(columnName).add(indexName);
        } else {
          HashSet<String> tmpSet = new HashSet<>();
          tmpSet.add(indexName);
          colIdxMap.put(columnName, tmpSet);
        }
        if (idxColMap.containsKey(indexName)) {
          idxColMap.get(indexName).add(columnName);
          continue;
        } else {
          HashSet<String> tmpSet = new HashSet<>();
          tmpSet.add(columnName);
          idxColMap.put(indexName, tmpSet);
        }
        AtlasCreateEntity indexEntity = new AtlasCreateEntity();
        indexEntity.setGuid(AtlasBaseEntity.nextInternalId());
        indexEntity.setTypeName(AtlasCreateEntity.RDBMS_INDEX);
        indexEntity.setVersion(0);
        HashMap<String, Object> indexAttributes = new HashMap<>();
        indexAttributes.put("name", resultSet.getString("index_name"));
        indexAttributes.put("description", resultSet.getString("index_comment"));
        indexAttributes.put("index_type", resultSet.getString("index_type"));
        indexAttributes.put("isUnique", resultSet.getInt("non_unique") == 0);
        indexAttributes.put("comment", resultSet.getString("index_comment"));
        indexAttributes.put(
            "qualifiedName",
            AtlasQualifiedNameUtils.getIdxQuaName(
                hostName, port, dbName, tabName, (String) indexAttributes.get("name")));
        indexEntity.setAttributes(indexAttributes);
        tableReferredEntities.put(indexEntity.getGuid(), indexEntity);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (queryIndexesPreSta != null) {
          queryIndexesPreSta.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private String getInstanceEntity(Map<String, AtlasCreateEntity> tableReferredEntities) {
    AtlasCreateEntity instanceEntity = new AtlasCreateEntity();
    instanceEntity.setGuid(AtlasBaseEntity.nextInternalId());
    instanceEntity.setTypeName(AtlasCreateEntity.RDBMS_INSTANCE);
    instanceEntity.setVersion(0);
    HashMap<String, Object> instanceAttributes = new HashMap<>();
    instanceAttributes.put(
        "qualifiedName", AtlasQualifiedNameUtils.getInstanceQuaName(hostName, port));
    instanceAttributes.put("name", AtlasQualifiedNameUtils.getInstanceQuaName(hostName, port));
    instanceAttributes.put("rdbms_type", "MySQL");
    instanceAttributes.put("hostname", mysqlAuthInfo.getAddress().getHostString());
    instanceAttributes.put("port", mysqlAuthInfo.getAddress().getPort());
    instanceEntity.setAttributes(instanceAttributes);
    tableReferredEntities.put(instanceEntity.getGuid(), instanceEntity);
    return instanceEntity.getGuid();
  }

  private void getDbEntity(
      Map<String, AtlasCreateEntity> tableReferredEntities, String dbName, String tabName) {
    String instanceGuid = getInstanceEntity(tableReferredEntities);
    AtlasCreateEntity dbEntity = new AtlasCreateEntity();
    dbEntity.setGuid(AtlasBaseEntity.nextInternalId());
    dbEntity.setTypeName(AtlasCreateEntity.RDBMS_DB);
    dbEntity.setVersion(0);
    HashMap<String, Object> dbAttributes = new HashMap<>();
    dbAttributes.put("qualifiedName", AtlasQualifiedNameUtils.getDbQuaName(hostName, port, dbName));
    dbAttributes.put("description", dbName);
    dbAttributes.put("name", dbName);
    dbAttributes.put(
        "instance", new AtlasReferredEntity(instanceGuid, AtlasCreateEntity.RDBMS_INSTANCE));
    dbEntity.setAttributes(dbAttributes);
    tableReferredEntities.put(dbEntity.getGuid(), dbEntity);
  }

  private void getColumnsEntities(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      String dbName,
      String tabName,
      Connection connection) {
    PreparedStatement queryColumnsPreSta = null;
    try {
      queryColumnsPreSta = connection.prepareStatement(SELECT_COLUMNS_INFO_SQL);
      queryColumnsPreSta.setString(1, dbName);
      queryColumnsPreSta.setString(2, tabName);
      ResultSet resultSet = queryColumnsPreSta.executeQuery();
      while (resultSet.next()) {
        AtlasCreateEntity columnEntity = new AtlasCreateEntity();
        columnEntity.setGuid(AtlasBaseEntity.nextInternalId());
        columnEntity.setTypeName(AtlasCreateEntity.RDBMS_COLUMN);
        columnEntity.setVersion(0);
        HashMap<String, Object> columnAttributes = new HashMap<>();
        columnAttributes.put("name", resultSet.getString("column_name"));
        columnAttributes.put("description", resultSet.getString("column_comment"));
        columnAttributes.put("data_type", resultSet.getString("column_type"));
        columnAttributes.put("length", resultSet.getInt("character_maximum_length"));
        columnAttributes.put("default_value", resultSet.getString("column_default"));
        columnAttributes.put("comment", resultSet.getString("column_comment"));
        columnAttributes.put(
            "isNullable", resultSet.getString("is_nullable").equalsIgnoreCase("YES"));
        columnAttributes.put(
            "isPrimaryKey", resultSet.getString("column_key").equalsIgnoreCase("PRI"));
        columnAttributes.put(
            "qualifiedName",
            AtlasQualifiedNameUtils.getColQuaName(
                hostName, port, dbName, tabName, (String) columnAttributes.get("name")));
        columnEntity.setAttributes(columnAttributes);
        tableReferredEntities.put(columnEntity.getGuid(), columnEntity);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (queryColumnsPreSta != null) {
          queryColumnsPreSta.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private List<AtlasBaseEntity> getTableEntities(
      String dbName,
      String tabName,
      Map<String, AtlasCreateEntity> tableReferredEntities,
      Connection connection) {
    PreparedStatement queryTablePreSta = null;
    AtlasCreateEntity tableEntity = new AtlasCreateEntity();
    try {
      queryTablePreSta = connection.prepareStatement(SELECT_TABLE_INFO_SQL);
      queryTablePreSta.setString(1, dbName);
      queryTablePreSta.setString(2, tabName);
      ResultSet resultSet = queryTablePreSta.executeQuery();
      tableEntity.setGuid(AtlasBaseEntity.nextInternalId());
      tableEntity.setTypeName(AtlasCreateEntity.RDBMS_TABLE);
      tableEntity.setVersion(0);
      while (resultSet.next()) {
        HashMap<String, Object> tableAttributes = new HashMap<>();
        //      String tableRows = resultSet.getString("table_rows");
        //      String tableAvgRowLen = resultSet.getString("avg_row_length");
        //      String tableUpdateTime = resultSet.getString("update_time");
        tableAttributes.put("name", resultSet.getString("table_name"));
        tableAttributes.put("description", resultSet.getString("table_comment"));
        tableAttributes.put(
            "createTime", Long.toString(resultSet.getDate("create_time").getTime()));
        tableAttributes.put("comment", resultSet.getString("table_comment"));
        tableAttributes.put("type", resultSet.getString("table_type"));
        tableAttributes.put(
            "qualifiedName",
            AtlasQualifiedNameUtils.getTabQueName(hostName, port, dbName, tabName));
        setRelatedAttributes(tableAttributes, tableReferredEntities, tableEntity.getGuid());
        tableEntity.setAttributes(tableAttributes);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (queryTablePreSta != null) {
          queryTablePreSta.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return Collections.singletonList(tableEntity);
  }

  private void setRelatedAttributes(
      HashMap<String, Object> tableAttributes,
      Map<String, AtlasCreateEntity> tableReferredEntities,
      String tabGuId) {
    List<AtlasReferredEntity> columns = new ArrayList<>();
    List<AtlasReferredEntity> indexes = new ArrayList<>();
    AtlasReferredEntity db = null;
    for (String guid : tableReferredEntities.keySet()) {
      String tableRefEntTypeName = tableReferredEntities.get(guid).getTypeName();
      if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_COLUMN)) {
        columns.add(new AtlasReferredEntity(guid, tableRefEntTypeName));
        setTabRelAtt(tableReferredEntities.get(guid), tabGuId);
      } else if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_DB)) {
        db = new AtlasReferredEntity(guid, tableRefEntTypeName);
      } else if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_INDEX)) {
        indexes.add(new AtlasReferredEntity(guid, tableRefEntTypeName));
        setTabRelAtt(tableReferredEntities.get(guid), tabGuId);
      }
    }
    if (!columns.isEmpty()) {
      tableAttributes.put("columns", columns);
    }
    if (!indexes.isEmpty()) {
      tableAttributes.put("indexes", indexes);
    }
    if (db != null) {
      tableAttributes.put("db", db);
    }
  }

  private void setTabRelAtt(AtlasCreateEntity columnEntity, String tabGuId) {
    columnEntity
        .getAttributes()
        .put("table", new AtlasReferredEntity(tabGuId, AtlasCreateEntity.RDBMS_TABLE));
  }
}
