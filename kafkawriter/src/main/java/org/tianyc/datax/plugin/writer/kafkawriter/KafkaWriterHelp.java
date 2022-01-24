package org.tianyc.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.*;

/**
 * @author ：Wangtao
 * @date ：Created in 2021/1/17 3:29 下午
 * @description：辅助类，解析Topic和拼接Binlog
 */

public class KafkaWriterHelp {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaWriterHelp.class);
    public static KafkaColumnCell kafkaColumnCell = null;
    private static Gson gson = new Gson();


    //解析COLUMN
    private static KafkaColumnCell parseOneColumn(String columnName) {
        Map<String, String> columnMap = new HashMap<>();
        columnMap.put(Key.NAME, columnName);
        columnMap.put(Key.VALUE, "");
        columnMap.put(Key.TYPE, "string");
        columnMap.put(Key.DATE_FORMAT, "");
        columnMap.put(Key.FIELD_TYPE, "");
        return parseOneColumn(columnMap);
    }

    private static KafkaColumnCell parseOneColumn(Map<String, String> columnInfo) {
        ColumnType columnType = ColumnType.VALUE;
        if (StringUtils.isNotBlank(columnInfo.get(Key.TYPE))) {
            columnType = ColumnType.getByTypeName(columnInfo.get(Key.TYPE));
        }

        String columnName = columnInfo.get(Key.NAME);
        String value = columnInfo.get(Key.VALUE);
        String dateFormat = columnInfo.get(Key.DATE_FORMAT);
        String fieldType = columnInfo.get(Key.FIELD_TYPE);

        KafkaColumnCell column = new KafkaColumnCell.Builder()
                .setDateFormat(dateFormat)
                .setType(columnType)
                .setValue(value)
                .setName(columnName)
                .setFieldType(fieldType)
                .build();
        return column;
    }


    public static List<KafkaColumnCell> parseColumn(List<Object> columnList) {
        List<KafkaColumnCell> list = new ArrayList<>();
        for (Object column : columnList) {
            if (column instanceof String) {
                list.add(KafkaWriterHelp.parseOneColumn((String) column));
            } else if (column instanceof Map) {
                list.add(KafkaWriterHelp.parseOneColumn((Map<String, String>) column));
            } else {
                throw DataXException.asDataXException(KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("kafkawriter 无法解析column"));
            }
        }
        return list;
    }

    public static Map<String, String> parseMySQLType(List<Object> columnList) {
        Map<String, String> mysqlTypeMap = new HashMap<>();
        for (Object column : columnList) {
            kafkaColumnCell = KafkaWriterHelp.parseOneColumn((Map<String, String>) column);
            mysqlTypeMap.put(kafkaColumnCell.getName(), kafkaColumnCell.getFieldType());
        }
        return mysqlTypeMap;
    }

    public static Map<String, Integer> parseSqlType(List<Object> columnList) throws IOException {
        Map<String, Integer> sqlTypeMap = new HashMap<>();
        for (Object column : columnList) {
            kafkaColumnCell = KafkaWriterHelp.parseOneColumn((Map<String, String>) column);
            if (kafkaColumnCell.getFieldType().contains("(")) {
                sqlTypeMap.put(kafkaColumnCell.getName(), Key.MAP.get(kafkaColumnCell.getFieldType().split("\\(")[0].toUpperCase()));
            } else if (kafkaColumnCell.getFieldType().contains("datetime")){
                sqlTypeMap.put(kafkaColumnCell.getName(), Key.MAP.get("TIMESTAMP".toUpperCase()));
            }
            else {
                sqlTypeMap.put(kafkaColumnCell.getName(), Key.MAP.get(kafkaColumnCell.getFieldType().toUpperCase()));
            }
        }
        return sqlTypeMap;
    }

    public static String transformBlog(String msg, List<Object> columnList,String database,String table,String pkNames) throws IOException {
        StringBuilder mysqlBinlog = new StringBuilder();
        String mysqlBin = mysqlBinlog.append("{\"data\":[").append(msg).append("],\"database\":").append("\""+database+"\"").append(",\"es\":").append(System.currentTimeMillis()).append(",\"id\":3004603,\"isDdl\":false,\"mysqlType\":")
                .append(gson.toJson(parseMySQLType(columnList))).append(",\"old\":null,\"pkNames\":[").append("\""+pkNames+"\"").append("],\"sql\":\"\",\"sqlType\":").append(gson.toJson(parseSqlType(columnList)))
                .append(",\"table\":").append("\""+table+"\"").append(",\"ts\":").append(System.currentTimeMillis()).append(",\"type\":\"REPLACE\"}").toString();
//        OneData oneData = new OneData(mysqlBin);
        return mysqlBin;
    }

    /**
     * 解析TOPIC
     *
     * @param topicList
     * @return
     */
    public static List<KafkaTopic> parseTopic(List<Object> topicList) {
        List<KafkaTopic> list = new ArrayList<>();
        for (Object topic : topicList) {
            if (topic instanceof String) {
                list.add(parseOneTopic((String) topic));
            } else if (topic instanceof Map) {
                list.add(parseOneTopic((Map<String, String>) topic));
            } else {
                throw DataXException.asDataXException(KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("kafkawriter 无法解析Topic"));
            }
        }
        return list;
    }

    private static KafkaTopic parseOneTopic(String topic) {
        Map<String, String> map = new HashMap<>(3);
        map.put(Key.NAME, topic);
        map.put(Key.PARTITION, "-1");
//        map.put(Key.MAX_PARTITION,"3");
        return parseOneTopic(map);
    }

    private static KafkaTopic parseOneTopic(Map<String, String> topic) {
        String topicName = topic.get(Key.NAME);
        int partition = 1;
        if (StringUtils.isNotBlank(topic.get(Key.PARTITION))
                && StringUtils.isNumeric(topic.get(Key.PARTITION))) {
            partition = Integer.parseInt(topic.get(Key.PARTITION));
        }
//        int maxPartition = 3;
//        if(StringUtils.isNotBlank(topic.get(Key.MAX_PARTITION))
//                && StringUtils.isNumeric(topic.get(Key.MAX_PARTITION))){
//            maxPartition = Integer.parseInt(topic.get(Key.MAX_PARTITION));
//        }

        KafkaTopic kafkaTopic = new KafkaTopic.Builder()
                .setName(topicName)
                .setPartation(partition)
//                .setMaxPartation(maxPartition)
                .build();
        return kafkaTopic;
    }
}
