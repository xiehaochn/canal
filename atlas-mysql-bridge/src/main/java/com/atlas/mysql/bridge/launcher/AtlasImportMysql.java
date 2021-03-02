package com.atlas.mysql.bridge.launcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.atlas.mysql.bridge.entity.base.AtlasHookMessage;
import com.atlas.mysql.bridge.utils.AtlasMessageUtils;
import com.atlas.mysql.bridge.utils.EmptySerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AtlasImportMysql {
  private static String host;
  private static String port;
  private static String user;
  private static String passWord;
  private static Producer<String, byte[]> producer;
  private static String topic;
  private static int partition;

  private static final String CLASSPATH_URL_PREFIX = "classpath:";
  private static final String INFORMATION_SCHEMA = "information_schema";
  private static final String SELECT_ALL_TABLE_SQL =
      "select table_schema,table_name from information_schema.tables where table_type='BASE TABLE'";

  private static final Logger logger = LoggerFactory.getLogger(AtlasImportMysql.class);

  public static void main(String[] args) {
    Properties properties = initProperties();
    Map<String, List<String>> regexMatchedTableList = initRegexMatchedTableList(properties);
    initKafkaProducer(properties);
    if (regexMatchedTableList != null) {
      initTableMetadata(regexMatchedTableList);
    } else {
      logger.error("##get table list failed");
    }
  }

  private static Properties initProperties() {
    Properties properties = new Properties();
    try {
      logger.info("## load atlas import mysql configurations");
      String conf = System.getProperty("atlas.conf", "classpath:atlas_mysql_import.properties");
      if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
        conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
        properties.load(AtlasImportMysql.class.getClassLoader().getResourceAsStream(conf));
      } else {
        properties.load(new FileInputStream(conf));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return properties;
  }

  private static Map<String, List<String>> initRegexMatchedTableList(Properties properties) {
    Map<String, List<String>> regexMatchedTableList = null;
    host = properties.getProperty("atlas.mysql.host");
    port = properties.getProperty("atlas.mysql.port");
    user = properties.getProperty("atlas.mysql.user");
    passWord = properties.getProperty("atlas.mysql.passWord");
    String dbRegex = properties.getProperty("atlas.mysql.db");
    String tableRegex = properties.getProperty("atlas.mysql.table");
    regexMatchedTableList = getDbTableList(dbRegex, tableRegex);
    logger.info(
        "## start to load following tables metadata:" + printTableList(regexMatchedTableList));
    return regexMatchedTableList;
  }

  private static void initKafkaProducer(Properties properties) {
    Properties kafkaProperties = new Properties();
    kafkaProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.bootstrap.servers"));
    kafkaProperties.put(ProducerConfig.ACKS_CONFIG, properties.getProperty("kafka.acks"));
    kafkaProperties.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty("kafka.compression.type"));
    kafkaProperties.put(
        ProducerConfig.BATCH_SIZE_CONFIG, properties.getProperty("kafka.batch.size"));
    kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, properties.getProperty("kafka.linger.ms"));
    kafkaProperties.put(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG, properties.getProperty("kafka.max.request.size"));
    kafkaProperties.put(
        ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getProperty("kafka.buffer.memory"));
    kafkaProperties.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        properties.getProperty("kafka.max.in.flight.requests.per.connection"));
    kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, properties.getProperty("kafka.retries"));
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmptySerializer.class);
    if (Boolean.parseBoolean(properties.getProperty("kafka.kerberos.enable"))) {
      File krb5File = new File(properties.getProperty("kafka.kerberos.krb5.file"));
      File jaasFile = new File(properties.getProperty("kafka.kerberos.jaas.file"));
      if (krb5File.exists() && jaasFile.exists()) {
        System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
        System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        kafkaProperties.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProperties.put("sasl.kerberos.service.name", "kafka");
        kafkaProperties.put("sasl.mechanism", "GSSAPI");
      }
    }
    topic = properties.getProperty("kafka.topic");
    partition = Integer.parseInt(properties.getProperty("kafka.partition"));
    producer = new KafkaProducer<>(kafkaProperties);
  }

  private static void initTableMetadata(Map<String, List<String>> regexMatchedTableList) {
    List<AtlasHookMessage> initMessageList = new ArrayList<>();
    AtlasMessageUtils atlasMessageUtils = new AtlasMessageUtils(host, port, user, passWord);
    for (String dbName : regexMatchedTableList.keySet()) {
      for (String tableName : regexMatchedTableList.get(dbName)) {
        initMessageList.addAll(atlasMessageUtils.convertToAtlasEntityMessage(dbName, tableName));
      }
    }
    ObjectMapper objectMapper = new ObjectMapper();
    if (!initMessageList.isEmpty()) {
      for (AtlasHookMessage atlasHookMessage : initMessageList) {
        try {
          logger.info(objectMapper.writeValueAsString(atlasHookMessage));
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      }
    } else {
      logger.info("initMessageList is empty");
    }
    List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
    if (!initMessageList.isEmpty()) {
      logger.info("kafka.topic:" + topic);
      logger.info("kafka.partition:" + partition);
      for (AtlasHookMessage atlasHookMessage : initMessageList) {
        records.add(
            new ProducerRecord<>(
                topic,
                partition,
                null,
                JSON.toJSONBytes(atlasHookMessage, SerializerFeature.WriteMapNullValue)));
      }
    }
    for (ProducerRecord<String, byte[]> record : records) {
      producer.send(record);
    }
    producer.flush();
  }

  private static String printTableList(Map<String, List<String>> regexMatchedTableList) {
    StringBuilder result = new StringBuilder();
    for (String dbName : regexMatchedTableList.keySet()) {
      result.append("database:").append(dbName).append("@").append("tables:");
      for (String tableName : regexMatchedTableList.get(dbName)) {
        result.append(tableName).append(",");
      }
    }
    return result.toString();
  }

  private static Map<String, List<String>> getDbTableList(String dbRegex, String tableRegex) {
    HashMap<String, List<String>> allDbTabList = new HashMap<>();
    Map<String, List<String>> tabRegexMatchedList = null;
    Connection connection = null;
    try {
      connection = initConnection(host, port, user, passWord);
      PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_TABLE_SQL);
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        String db = resultSet.getString("table_schema");
        String table = resultSet.getString("table_name");
        if (!allDbTabList.containsKey(db)) {
          allDbTabList.put(db, new ArrayList<>());
        }
        allDbTabList.get(db).add(table);
      }
      Map<String, List<String>> dbRegexMatchedList = matchDbRegex(allDbTabList, dbRegex);
      tabRegexMatchedList = matchTabRegex(dbRegexMatchedList, tableRegex);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return tabRegexMatchedList;
  }

  private static Map<String, List<String>> matchTabRegex(
      Map<String, List<String>> dbRegexMatchedList, String tableRegex) {
    HashMap<String, List<String>> tabRegexMatchedList = new HashMap<>();
    Pattern tabPattern = Pattern.compile(tableRegex);
    for (String dbName : dbRegexMatchedList.keySet()) {
      List<String> tables = dbRegexMatchedList.get(dbName);
      List<String> tablesMatched = new ArrayList<>();
      for (String tableName : tables) {
        Matcher tabMatcher = tabPattern.matcher(tableName);
        if (tabMatcher.matches()) {
          tablesMatched.add(tableName);
        }
      }
      tabRegexMatchedList.put(dbName, tablesMatched);
    }
    return tabRegexMatchedList;
  }

  private static Map<String, List<String>> matchDbRegex(
      Map<String, List<String>> dbTableList, String dbRegex) {
    HashMap<String, List<String>> dbRegexMatchedList = new HashMap<>();
    Pattern dbPattern = Pattern.compile(dbRegex);
    for (String dbName : dbTableList.keySet()) {
      Matcher dbMatcher = dbPattern.matcher(dbName);
      if (dbMatcher.matches()) {
        dbRegexMatchedList.put(dbName, dbTableList.get(dbName));
      }
    }
    return dbRegexMatchedList;
  }

  private static Connection initConnection(String host, String port, String user, String passWord)
      throws Exception {
    String source =
        "jdbc:mysql://"
            + host
            + ":"
            + port
            + "/"
            + INFORMATION_SCHEMA
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    return DriverManager.getConnection(source, user, passWord);
  }
}
