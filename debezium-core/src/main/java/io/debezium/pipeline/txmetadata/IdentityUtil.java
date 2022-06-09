/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

public class IdentityUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityUtil.class);
    public static CommonConnectorConfig commonConnectorConfig;

    public static final String CHANGE_NUMBERS_TALBE = "CHANGE_NUMBERS";
    private Map<String, String> tableIdentityIdColumn;
    private Set<String> entityTypes;

    public IdentityUtil(CommonConnectorConfig connectorConfig) {
        IdentityUtil.commonConnectorConfig = connectorConfig;
        String transactionSplitterConfigLocation = connectorConfig.getTransactionSplitterConfigLocation();
        if (!Strings.isNullOrBlank(transactionSplitterConfigLocation)) {
            LOGGER.info("Transaction splitter configuration file {}", transactionSplitterConfigLocation);
            tableIdentityIdColumn = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode result = mapper.readTree(Paths.get(transactionSplitterConfigLocation).toFile());
                List<JsonNode> tablesNodes = result.findValues("tables");

                for (JsonNode tables : tablesNodes) {
                    Iterator<JsonNode> ite = tables.iterator();
                    while (ite.hasNext()) {
                        JsonNode table = ite.next();
                        String key = table.get("tableName").asText();
                        String value = table.get("idColumn").asText();
                        tableIdentityIdColumn.put(key, value);
                    }
                }

                tableIdentityIdColumn.put(CHANGE_NUMBERS_TALBE, "ENTITY_ID");

                entityTypes = new HashSet<>();
                List<JsonNode> entityNodes = result.findValues("entityConfigs");
                Iterator<String> ite = entityNodes.get(0).fieldNames();
                while (ite.hasNext()) {
                    entityTypes.add(ite.next().trim().toLowerCase());
                }

                LOGGER.info("Loaded transaction split configuration {}, entityTypes {}", tableIdentityIdColumn, entityTypes);
            }
            catch (Exception e) {
                LOGGER.error("Exception when parsing transactionSplitter configuration file ", transactionSplitterConfigLocation, e);
            }
        }
    }

    public boolean isTransactionSplitEnabled() {
        return tableIdentityIdColumn != null;
    }

    public String getIdentityId(String dataCollectionName, Struct value) {
        if (tableIdentityIdColumn == null) {
            return null;
        }

        String tableName = getTableName(dataCollectionName);
        String idColumnName = tableIdentityIdColumn.get(tableName);
        if (idColumnName == null) {
            LOGGER.trace("Didn't find identity column name for table {} {}", tableName, dataCollectionName);
            return null;
        }
        String identityId = getColumnValue(value, idColumnName);
        if (identityId == null) {
            if (entityTypes.contains(tableName.toLowerCase())) {
                LOGGER.error("Can't find identityId  with columnName {} from {}", idColumnName, value);
            }
        }
        return identityId;
    }

    private String getTableName(String dataCollectionName) {
        String tableName = dataCollectionName;
        int index = dataCollectionName.lastIndexOf(".");
        if (index >= 0) {
            tableName = dataCollectionName.substring(index + 1);
        }

        return TableId.transformPostgresPartitionTableName(tableName);
    }

    private String getColumnValue(Struct value, String column) {
        Struct table = value.getStruct("after");
        if (table == null) {
            table = value.getStruct("before");
        }
        return table != null ? table.getString(column) : null;
    }
}
