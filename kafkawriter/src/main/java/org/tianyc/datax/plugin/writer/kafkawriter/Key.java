package org.tianyc.datax.plugin.writer.kafkawriter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：Wangtao
 * @date ：Created in 2021/1/17 3:29 下午
 * @description：常量类
 */

public class Key {
    public final static String CONFIG = "config";
    public final static String COLUMN = "column";
    public final static String NAME = "name";
    public final static String TYPE = "type";

    public final static String DATE_FORMAT = "dateFormat";
    public final static String VALUE = "value";
    public final static String FIELD_TYPE = "fieldType";

    public final static String TOPIC = "topic";
    public final static String PARTITION = "partition";
//    public final static String MAX_PARTITION = "maxPartition";

    //默认值：256
    public final static String BATCH_SIZE = "batchSize";

    //默认值：32m
    public final static String BATCH_BYTE_SIZE = "batchByteSize";
    public final static Map<String, Integer> MAP = new HashMap<String, Integer>();

    static {
        MAP.put("BIT", -7);
        MAP.put("TINYINT", -6);
        MAP.put("SMALLINT", 5);
        MAP.put("INTEGER", 4);
        MAP.put("BIGINT", -5);
        MAP.put("FLOAT", 6);
        MAP.put("REAL", 7);
        MAP.put("DOUBLE", 8);
        MAP.put("NUMERIC", 2);
        MAP.put("DECIMAL", 3);
        MAP.put("CHAR", 1);
        MAP.put("VARCHAR", 12);
        MAP.put("LONGVARCHAR", -1);
        MAP.put("DATE", 91);
        MAP.put("TIME", 92);
        MAP.put("TIMESTAMP", 93);
        MAP.put("BINARY", -2);
        MAP.put("VARBINARY", -3);
        MAP.put("LONGVARBINARY", -4);
        MAP.put("NULL", 0);
        MAP.put("OTHER", 1111);
        MAP.put("JAVA_OBJECT", 2000);
        MAP.put("DISTINCT", 2001);
        MAP.put("STRUCT", 2002);
        MAP.put("ARRAY", 2003);
        MAP.put("BLOB", 2004);
        MAP.put("CLOB", 2005);
        MAP.put("REF", 2006);
        MAP.put("DATALINK", 70);
        MAP.put("BOOLEAN", 16);
        MAP.put("ROWID", -8);
        MAP.put("NCHAR", -15);
        MAP.put("NVARCHAR", -9);
        MAP.put("LONGNVARCHAR", -16);
        MAP.put("NCLOB", 2011);
        MAP.put("SQLXML", 2009);
    }


}



