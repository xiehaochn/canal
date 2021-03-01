package com.alibaba.otter.canal.connector.core.producer.atlas;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.atlas.base.*;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateEntities;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateEntity;
import com.alibaba.otter.canal.protocol.atlas.create.AtlasCreateMessage;
import com.alibaba.otter.canal.protocol.atlas.delete.AtlasDeleteEntity;
import com.alibaba.otter.canal.protocol.atlas.delete.AtlasDeleteMessage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AtlasMessageUtils {
  private AuthenticationInfo mysqlAuthInfo;

  private String hostName;

  private String port;

  private String user;

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
    this.user = mysqlAuthInfo.getUsername();
  }

  public List<AtlasHookMessage> convertToAtlasEntityMessage(FlatMessage flatMessage) {
    switch (flatMessage.getType()) {
      case "CREATE":
        return getTabHookMassage(
            flatMessage.getDatabase().toLowerCase(),
            flatMessage.getTable().toLowerCase(),
            HookNotificationType.ENTITY_CREATE_V2);
      case "RENAME":
        return renameTabMessage(parseTabName(flatMessage), flatMessage);
      case "ALTER":
        return getTabHookMassage(
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
        return Collections.emptyList();
    }
  }

  private List<AtlasHookMessage> getTabHookMassage(
      String dbName, String tabName, HookNotificationType messageType) {
    AtlasCreateMessage tabMessage = new AtlasCreateMessage();
    tabMessage.setType(messageType);
    tabMessage.setUser(user);
    tabMessage.setEntities(getTabEntities(dbName, tabName));
    AtlasHookMessage tabHookMessage = new AtlasHookMessage();
    setCommonProperties(tabHookMessage);
    tabHookMessage.setMessage(tabMessage);
    return Collections.singletonList(tabHookMessage);
  }

  private AtlasCreateEntities getTabEntities(String dbName, String tabName) {
    Connection connection = null;
    AtlasCreateEntities tabCreateEntities = new AtlasCreateEntities();
    try {
      connection = initConnection(mysqlAuthInfo);
      Map<String, AtlasCreateEntity> tabRefEntities =
          getTableReferredEntities(dbName, tabName, connection);
      List<AtlasBaseEntity> tabEntities = getTabSecEntities(dbName, tabName, connection);
      setRelatedAttributesToTab(tabEntities, tabRefEntities);
      tabCreateEntities.setEntities(tabEntities);
      tabCreateEntities.setReferredEntities(tabRefEntities);
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
    return tabCreateEntities;
  }

  private Map<String, AtlasCreateEntity> getTableReferredEntities(
      String dbName, String tabName, Connection connection) {
    Map<String, AtlasCreateEntity> tabRefEntities = new HashMap<>();
    HashMap<String, HashSet<String>> idxColMap = new HashMap<>();
    HashMap<String, HashSet<String>> colIdxMap = new HashMap<>();
    getColEntities(tabRefEntities, dbName, tabName, connection);
    getIdxEntities(tabRefEntities, dbName, tabName, connection, idxColMap, colIdxMap);
    String insEntGuid = getInstanceEntity(tabRefEntities);
    getDbEntity(tabRefEntities, dbName, insEntGuid);
    setColIdxRel(tabRefEntities, idxColMap, colIdxMap);
    return tabRefEntities;
  }

  private void getColEntities(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      String dbName,
      String tabName,
      Connection connection) {
    List<AtlasCreateEntity> colEntList =
        AtlasAttributesQueryUtils.getAttFromSchema(
            connection,
            SELECT_COLUMNS_INFO_SQL,
            new AtlasAttributesQueryUtils.TabInfo(hostName, port, dbName, tabName),
            AtlasCreateEntity.RDBMS_COLUMN,
            null,
            null);
    for (AtlasCreateEntity colEnt : colEntList) {
      tableReferredEntities.put(colEnt.getGuid(), colEnt);
    }
  }

  private void getIdxEntities(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      String dbName,
      String tabName,
      Connection connection,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap) {
    List<AtlasCreateEntity> idxEntList =
        AtlasAttributesQueryUtils.getAttFromSchema(
            connection,
            SELECT_INDEX_INFO_SQL,
            new AtlasAttributesQueryUtils.TabInfo(hostName, port, dbName, tabName),
            AtlasCreateEntity.RDBMS_INDEX,
            idxColMap,
            colIdxMap);
    for (AtlasCreateEntity idxEnt : idxEntList) {
      tableReferredEntities.put(idxEnt.getGuid(), idxEnt);
    }
  }

  private String getInstanceEntity(Map<String, AtlasCreateEntity> tableReferredEntities) {
    HashMap<String, Object> insAttributes = new HashMap<>();
    insAttributes.put("qualifiedName", AtlasQualifiedNameUtils.getInstanceQuaName(hostName, port));
    insAttributes.put("name", AtlasQualifiedNameUtils.getInstanceQuaName(hostName, port));
    insAttributes.put("rdbms_type", "MySQL");
    insAttributes.put("hostname", hostName);
    insAttributes.put("port", port);
    AtlasCreateEntity insEntity = new AtlasCreateEntity();
    insEntity.setGuid(AtlasBaseEntity.nextInternalId());
    insEntity.setTypeName(AtlasCreateEntity.RDBMS_INSTANCE);
    insEntity.setVersion(0);
    insEntity.setAttributes(insAttributes);
    tableReferredEntities.put(insEntity.getGuid(), insEntity);
    return insEntity.getGuid();
  }

  private void getDbEntity(
      Map<String, AtlasCreateEntity> tableReferredEntities, String dbName, String insEntGuid) {
    HashMap<String, Object> dbAttributes = new HashMap<>();
    dbAttributes.put("qualifiedName", AtlasQualifiedNameUtils.getDbQuaName(hostName, port, dbName));
    dbAttributes.put("description", dbName);
    dbAttributes.put("name", dbName);
    dbAttributes.put(
        "instance", new AtlasReferredEntity(insEntGuid, AtlasCreateEntity.RDBMS_INSTANCE));
    AtlasCreateEntity dbEntity = new AtlasCreateEntity();
    dbEntity.setGuid(AtlasBaseEntity.nextInternalId());
    dbEntity.setTypeName(AtlasCreateEntity.RDBMS_DB);
    dbEntity.setVersion(0);
    dbEntity.setAttributes(dbAttributes);
    tableReferredEntities.put(dbEntity.getGuid(), dbEntity);
  }

  private void setColIdxRel(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> idxColMap,
      HashMap<String, HashSet<String>> colIdxMap) {
    replaceNameWithGuid(tableReferredEntities, idxColMap, AtlasCreateEntity.RDBMS_COLUMN);
    replaceNameWithGuid(tableReferredEntities, colIdxMap, AtlasCreateEntity.RDBMS_INDEX);
    addRelationships(
        tableReferredEntities,
        idxColMap,
        AtlasCreateEntity.RDBMS_INDEX,
        AtlasCreateEntity.RDBMS_COLUMN,
        "columns");
    addRelationships(
        tableReferredEntities,
        colIdxMap,
        AtlasCreateEntity.RDBMS_COLUMN,
        AtlasCreateEntity.RDBMS_INDEX,
        "indexes");
  }

  private void replaceNameWithGuid(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> relMap,
      String entType) {
    for (String guid : tableReferredEntities.keySet()) {
      AtlasCreateEntity entity = tableReferredEntities.get(guid);
      if (!entity.getTypeName().equalsIgnoreCase(entType)) {
        continue;
      }
      String name = (String) entity.getAttributes().get("name");
      for (HashSet<String> values : relMap.values()) {
        if (values.contains(name)) {
          values.remove(name);
          values.add(guid);
        }
      }
    }
  }

  private void addRelationships(
      Map<String, AtlasCreateEntity> tableReferredEntities,
      HashMap<String, HashSet<String>> relMap,
      String mainType,
      String relationType,
      String attName) {
    for (String guid : tableReferredEntities.keySet()) {
      AtlasCreateEntity entity = tableReferredEntities.get(guid);
      if (!entity.getTypeName().equalsIgnoreCase(mainType)) {
        continue;
      }
      String name = (String) entity.getAttributes().get("name");
      if (relMap.containsKey(name)) {
        addRelAttForOneEnt(entity, relMap.get(name), attName, relationType);
      }
    }
  }

  private void addRelAttForOneEnt(
      AtlasCreateEntity entity, HashSet<String> guidSet, String attName, String relationType) {
    List<AtlasReferredEntity> atlasReferredEntities = new ArrayList<>();
    for (String guid : guidSet) {
      atlasReferredEntities.add(new AtlasReferredEntity(guid, relationType));
    }
    entity.getAttributes().put(attName, atlasReferredEntities);
  }

  private List<AtlasBaseEntity> getTabSecEntities(
      String dbName, String tabName, Connection connection) {
    List<AtlasCreateEntity> tabEntList =
        AtlasAttributesQueryUtils.getAttFromSchema(
            connection,
            SELECT_TABLE_INFO_SQL,
            new AtlasAttributesQueryUtils.TabInfo(hostName, port, dbName, tabName),
            AtlasCreateEntity.RDBMS_TABLE,
            null,
            null);
    return Collections.singletonList(tabEntList.get(0));
  }

  private void setRelatedAttributesToTab(
      List<AtlasBaseEntity> tableEntities, Map<String, AtlasCreateEntity> tableReferredEntities) {
    AtlasCreateEntity tabEnt = (AtlasCreateEntity) tableEntities.get(0);
    List<AtlasReferredEntity> columns = new ArrayList<>();
    List<AtlasReferredEntity> indexes = new ArrayList<>();
    AtlasReferredEntity db = null;
    for (String guid : tableReferredEntities.keySet()) {
      String tableRefEntTypeName = tableReferredEntities.get(guid).getTypeName();
      if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_COLUMN)) {
        columns.add(new AtlasReferredEntity(guid, tableRefEntTypeName));
        setTabRelAtt(tableReferredEntities.get(guid), tabEnt.getGuid());
      } else if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_DB)) {
        db = new AtlasReferredEntity(guid, tableRefEntTypeName);
      } else if (tableRefEntTypeName.equals(AtlasCreateEntity.RDBMS_INDEX)) {
        indexes.add(new AtlasReferredEntity(guid, tableRefEntTypeName));
        setTabRelAtt(tableReferredEntities.get(guid), tabEnt.getGuid());
      }
    }
    if (!columns.isEmpty()) {
      tabEnt.getAttributes().put("columns", columns);
    }
    if (!indexes.isEmpty()) {
      tabEnt.getAttributes().put("indexes", indexes);
    }
    if (db != null) {
      tabEnt.getAttributes().put("db", db);
    }
  }

  private void setTabRelAtt(AtlasCreateEntity entity, String tabGuId) {
    entity
        .getAttributes()
        .put("table", new AtlasReferredEntity(tabGuId, AtlasCreateEntity.RDBMS_TABLE));
  }

  private HashMap<String, String> parseTabName(FlatMessage flatMessage) {
    HashMap<String, String> tabMap = new HashMap<>();
    String sql = flatMessage.getSql().replaceAll("'", "").toLowerCase();
    Pattern alterPattern = Pattern.compile("alter\\s*table");
    Matcher alterMatcher = alterPattern.matcher(sql);
    if (alterMatcher.find()) {
      Pattern patternAlter = Pattern.compile("[(\\s)*,][\\w.]*\\s*rename\\s*(to)?\\s*[\\w.]*");
      Matcher matcherAlter = patternAlter.matcher(sql);
      while (matcherAlter.find()) {
        String tabNameString = matcherAlter.group();
        System.out.println(tabNameString);
        String[] tabNames = tabNameString.split("\\s*rename\\s*(to)?\\s*");
        tabMap.put(tabNames[0].replaceAll("\\s", ""), tabNames[1].replaceAll("\\s", ""));
      }
    } else {
      Pattern patternRename = Pattern.compile("[(\\s*),][\\w.]*\\s*to\\s*[\\w.]*");
      Matcher matcherRename = patternRename.matcher(sql);
      System.out.println(matcherRename.groupCount());
      while (matcherRename.find()) {
        String tabNameString = matcherRename.group();
        System.out.println(tabNameString);
        String[] tabNames = tabNameString.split("\\s*to\\s*");
        tabMap.put(tabNames[0].replaceAll("\\s", ""), tabNames[1].replaceAll("\\s", ""));
      }
    }
    return tabMap;
  }

  private List<AtlasHookMessage> renameTabMessage(
      HashMap<String, String> tabMap, FlatMessage flatMessage) {
    List<AtlasHookMessage> rTabMessages = new ArrayList<>();
    for (String oldName : tabMap.keySet()) {
      String newName = tabMap.get(oldName);
      String oldTabName, oldDbName, newTabName, newDbName;
      if (oldName.contains(".")) {
        String[] oldNames = oldName.split(".");
        oldDbName = oldNames[0];
        oldTabName = oldNames[1];
      } else {
        oldDbName = flatMessage.getDatabase().toLowerCase();
        oldTabName = oldName;
      }
      if (newName.contains(".")) {
        String[] newNames = newName.split(".");
        newDbName = newNames[0];
        newTabName = newNames[1];
      } else {
        newDbName = flatMessage.getDatabase().toLowerCase();
        newTabName = newName;
      }
      rTabMessages.addAll(eraseTableMassage(oldDbName, oldTabName));
      rTabMessages.addAll(
          getTabHookMassage(newDbName, newTabName, HookNotificationType.ENTITY_CREATE_V2));
    }
    return rTabMessages;
  }

  private List<AtlasHookMessage> deleteIndexMessage(String db, String table, String sql) {
    List<AtlasHookMessage> dIdxHookMessages = new ArrayList<>();
    String idxName = "";
    String pattern = "index\\s.*\\son";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(sql.replaceAll("'", "").toLowerCase());
    while (m.find()) {
      String idxNameString = m.group();
      String[] idxNames = idxNameString.split("\\s");
      idxName = idxNames[1];
    }
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put(
        "qualifiedName",
        AtlasQualifiedNameUtils.getIdxQuaName(
            new AtlasAttributesQueryUtils.TabInfo(hostName, port, db, table), idxName));
    AtlasDeleteEntity atlasDeleteEntity = new AtlasDeleteEntity();
    atlasDeleteEntity.setUniqueAttributes(hashMap);
    atlasDeleteEntity.setTypeName(AtlasCreateEntity.RDBMS_INDEX);
    AtlasDeleteMessage dIdxMessage = new AtlasDeleteMessage();
    dIdxMessage.setType(HookNotificationType.ENTITY_DELETE_V2);
    dIdxMessage.setUser(mysqlAuthInfo.getUsername());
    dIdxMessage.setEntities(Collections.singletonList(atlasDeleteEntity));
    AtlasHookMessage dIdxHookMessage = new AtlasHookMessage();
    setCommonProperties(dIdxHookMessage);
    dIdxHookMessage.setMessage(dIdxMessage);
    dIdxHookMessages.add(dIdxHookMessage);
    dIdxHookMessages.addAll(
        getTabHookMassage(db, table, HookNotificationType.ENTITY_FULL_UPDATE_V2));
    return dIdxHookMessages;
  }

  private List<AtlasHookMessage> createIndexMessage(String db, String table) {
    List<AtlasBaseEntity> idxSecEntities = new ArrayList<>();
    Map<String, AtlasCreateEntity> refEntities = new HashMap<>();
    AtlasHookMessage tabHookMassage =
        getTabHookMassage(db, table, HookNotificationType.ENTITY_FULL_UPDATE_V2).get(0);
    for (AtlasBaseEntity entity :
        ((AtlasCreateMessage) tabHookMassage.getMessage()).getEntities().getEntities()) {
      refEntities.put(((AtlasCreateEntity) entity).getGuid(), (AtlasCreateEntity) entity);
    }
    for (AtlasBaseEntity entity :
        ((AtlasCreateMessage) tabHookMassage.getMessage())
            .getEntities()
            .getReferredEntities()
            .values()) {
      if (entity.getTypeName().equals(AtlasCreateEntity.RDBMS_INDEX)) {
        idxSecEntities.add(entity);
      } else {
        refEntities.put(((AtlasCreateEntity) entity).getGuid(), (AtlasCreateEntity) entity);
      }
    }
    AtlasCreateEntities cIdxEntities = new AtlasCreateEntities();
    cIdxEntities.setEntities(idxSecEntities);
    cIdxEntities.setReferredEntities(refEntities);
    AtlasCreateMessage cIdxMessage = new AtlasCreateMessage();
    cIdxMessage.setType(HookNotificationType.ENTITY_CREATE_V2);
    cIdxMessage.setUser(mysqlAuthInfo.getUsername());
    cIdxMessage.setEntities(cIdxEntities);
    AtlasHookMessage cIdxHookMessage = new AtlasHookMessage();
    setCommonProperties(cIdxHookMessage);
    cIdxHookMessage.setMessage(cIdxMessage);
    return Collections.singletonList(cIdxHookMessage);
  }

  private List<AtlasHookMessage> eraseTableMassage(String dbName, String tabName) {
    AtlasDeleteMessage eTabMessage = new AtlasDeleteMessage();
    eTabMessage.setType(HookNotificationType.ENTITY_DELETE_V2);
    eTabMessage.setUser(user);
    eTabMessage.setEntities(getDropTabSecEntities(dbName, tabName));
    AtlasHookMessage eTableMassage = new AtlasHookMessage();
    setCommonProperties(eTableMassage);
    eTableMassage.setMessage(eTabMessage);
    return Collections.singletonList(eTableMassage);
  }

  private List<AtlasDeleteEntity> getDropTabSecEntities(String dbName, String tabName) {
    HashMap<String, String> dropUniAtt = new HashMap<>();
    dropUniAtt.put(
        "qualifiedName",
        AtlasQualifiedNameUtils.getTabQueName(
            new AtlasAttributesQueryUtils.TabInfo(hostName, port, dbName, tabName)));
    AtlasDeleteEntity dropTabEntities = new AtlasDeleteEntity();
    dropTabEntities.setTypeName(AtlasCreateEntity.RDBMS_TABLE);
    dropTabEntities.setUniqueAttributes(dropUniAtt);
    return Collections.singletonList(dropTabEntities);
  }

  private void setCommonProperties(AtlasHookMessage atlasHookMessage) {
    atlasHookMessage.setMsgCompressionKind("NONE");
    atlasHookMessage.setMsgCreatedBy("MySQL");
    atlasHookMessage.setMsgCreationTime(System.currentTimeMillis());
    atlasHookMessage.setMsgSourceIP(mysqlAuthInfo.getAddress().getHostString());
    atlasHookMessage.setMsgSplitCount(1);
    atlasHookMessage.setMsgSplitIdx(1);
    atlasHookMessage.setVersion(new Version("1.0.0", Collections.singletonList(1)));
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
}
