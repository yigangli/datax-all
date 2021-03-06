package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.mysql.jdbc.JDBC4PreparedStatement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.zip.CRC32;
import java.util.zip.Checksum;


public class AdsInsertProxy implements AdsProxy {

    private static final Logger LOG = LoggerFactory
            .getLogger(AdsInsertProxy.class);
    private static final boolean IS_DEBUG_ENABLE = LOG.isDebugEnabled();
    private static final int MAX_EXCEPTION_CAUSE_ITER = 100;

    private String table;
    private List<String> columns;
    private TaskPluginCollector taskPluginCollector;
    private Configuration configuration;
    private Boolean emptyAsNull;

    private String writeMode;

    private String insertSqlPrefix;
    private String deleteSqlPrefix;
    private int opColumnIndex;
    private String lastDmlMode;
    // columnName: <java sql type, ads type name>
    private Map<String, Pair<Integer, String>> adsTableColumnsMetaData;
    private Map<String, Pair<Integer, String>> userConfigColumnsMetaData;
    // columnName: index @ ads column
    private Map<String, Integer> primaryKeyNameIndexMap;

    private int retryTimeUpperLimit;
    private Connection currentConnection;

    private String partitionColumn;
    private int partitionColumnIndex = -1;
    private int partitionCount;

    public AdsInsertProxy(String table, List<String> columns, Configuration configuration, TaskPluginCollector taskPluginCollector, TableInfo tableInfo) {
        this.table = table;
        this.columns = columns;
        this.configuration = configuration;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = configuration.getBool(Key.EMPTY_AS_NULL, false);
        this.writeMode = configuration.getString(Key.WRITE_MODE);
        this.insertSqlPrefix = String.format(Constant.INSERT_TEMPLATE, this.table, StringUtils.join(columns, ","));
        this.deleteSqlPrefix = String.format(Constant.DELETE_TEMPLATE, this.table);
        this.opColumnIndex = configuration.getInt(Key.OPIndex, 0);
        this.retryTimeUpperLimit = configuration.getInt(
                Key.RETRY_CONNECTION_TIME, Constant.DEFAULT_RETRY_TIMES);
        this.partitionCount = tableInfo.getPartitionCount();
        this.partitionColumn = tableInfo.getPartitionColumn();

        //??????ads?????????????????????????????????????????????select colums from table where 1=2?????????????????????????????????ads????????????
        //not this: this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, StringUtils.join(this.columns, ","));
        //no retry here(fetch meta data) ?????????????????????????????????
        this.adsTableColumnsMetaData = AdsInsertUtil.getColumnMetaData(tableInfo, this.columns);
        this.userConfigColumnsMetaData = new HashMap<String, Pair<Integer, String>>();

        List<String> primaryKeyColumnName = tableInfo.getPrimaryKeyColumns();
        List<String> adsColumnsNames = tableInfo.getColumnsNames();
        this.primaryKeyNameIndexMap = new HashMap<String, Integer>();
        //warn: ????????????????????????column??????, ???????????????ads??????????????????column??????, ????????????load???????????????????????????
        for (int i = 0; i < this.columns.size(); i++) {
            String oriEachColumn = this.columns.get(i);
            String eachColumn = oriEachColumn;
            // ??????????????????
            if (eachColumn.startsWith(Constant.ADS_QUOTE_CHARACTER) && eachColumn.endsWith(Constant.ADS_QUOTE_CHARACTER)) {
                eachColumn = eachColumn.substring(1, eachColumn.length() - 1);
            }
            for (String eachPrimary : primaryKeyColumnName) {
                if (eachColumn.equalsIgnoreCase(eachPrimary)) {
                    this.primaryKeyNameIndexMap.put(oriEachColumn, i);
                }
            }
            for (String eachAdsColumn : adsColumnsNames) {
                if (eachColumn.equalsIgnoreCase(eachAdsColumn)) {
                    this.userConfigColumnsMetaData.put(oriEachColumn, this.adsTableColumnsMetaData.get(eachAdsColumn));
                }
            }

            // ???????????????column??????????????????ads????????????????????????????????????256?????????
            if (eachColumn.equalsIgnoreCase(this.partitionColumn)) {
                this.partitionColumnIndex = i;
            }
        }
    }

    public void startWriteWithConnection(RecordReceiver recordReceiver,
                                         Connection connection,
                                         int columnNumber) {
        this.currentConnection = connection;
        int batchSize = this.configuration.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        // ???????????????bufferSize?????????batchSize??????
        int bufferSize = this.configuration.getInt(Key.BUFFER_SIZE, batchSize);
        // insert??????????????????????????????insert???????????????ads
        List<Record> writeBuffer = new ArrayList<Record>(bufferSize);
        List<Record> deleteBuffer = null;
        if (this.writeMode.equalsIgnoreCase(Constant.STREAMMODE)) {
            // delete??????????????????????????????delete???????????????ads
            deleteBuffer = new ArrayList<Record>(bufferSize);
        }
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (this.writeMode.equalsIgnoreCase(Constant.INSERTMODE)) {
                    if (record.getColumnNumber() != columnNumber) {
                        // ??????????????????????????????????????????????????????????????????????????????
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "????????????????????????. ???????????????????????????????????????????????????:%s ??? ??????????????????????????????:%s ?????????. ????????????????????????????????????.",
                                                record.getColumnNumber(),
                                                columnNumber));
                    }
                    writeBuffer.add(record);
                    if (writeBuffer.size() >= bufferSize) {
                        this.doBatchRecordWithPartitionSort(writeBuffer, Constant.INSERTMODE, bufferSize, batchSize);
                        writeBuffer.clear();
                    }
                } else {
                    if (record.getColumnNumber() != columnNumber + 1) {
                        // ????????????????????????????????????????????????????????????+1, ????????????, ??????????????????OP
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "????????????????????????. ???????????????????????????????????????????????????:%s ??? ??????????????????????????????:%s ??????????????????1??????????????????. ????????????????????????????????????.",
                                                record.getColumnNumber(),
                                                columnNumber));
                    }
                    String optionColumnValue = record.getColumn(this.opColumnIndex).asString();
                    OperationType operationType = OperationType.asOperationType(optionColumnValue);
                    if (operationType.isInsertTemplate()) {
                        writeBuffer.add(record);
                        if (this.lastDmlMode == null || this.lastDmlMode == Constant.INSERTMODE) {
                            this.lastDmlMode = Constant.INSERTMODE;
                            if (writeBuffer.size() >= bufferSize) {
                                this.doBatchRecordWithPartitionSort(writeBuffer, Constant.INSERTMODE, bufferSize, batchSize);
                                writeBuffer.clear();
                            }
                        } else {
                            this.lastDmlMode = Constant.INSERTMODE;
                            // ??????????????????????????????ads delete, ?????????insert??????
                            this.doBatchRecordWithPartitionSort(deleteBuffer, Constant.DELETEMODE, bufferSize, batchSize);
                            deleteBuffer.clear();
                        }
                    } else if (operationType.isDeleteTemplate()) {
                        deleteBuffer.add(record);
                        if (this.lastDmlMode == null || this.lastDmlMode == Constant.DELETEMODE) {
                            this.lastDmlMode = Constant.DELETEMODE;
                            if (deleteBuffer.size() >= bufferSize) {
                                this.doBatchRecordWithPartitionSort(deleteBuffer, Constant.DELETEMODE, bufferSize, batchSize);
                                deleteBuffer.clear();
                            }
                        } else {
                            this.lastDmlMode = Constant.DELETEMODE;
                            // ??????????????????????????????ads insert, ?????????delete??????
                            this.doBatchRecordWithPartitionSort(writeBuffer, Constant.INSERTMODE, bufferSize, batchSize);
                            writeBuffer.clear();
                        }
                    } else {
                        // ??????OP????????????????????????, ?????????????????????
                        this.taskPluginCollector.collectDirtyRecord(record, String.format("???????????????????????????:%s", optionColumnValue));
                    }
                }
            }

            if (!writeBuffer.isEmpty()) {
                //doOneRecord(writeBuffer, Constant.INSERTMODE);
                this.doBatchRecordWithPartitionSort(writeBuffer, Constant.INSERTMODE, bufferSize, batchSize);
                writeBuffer.clear();
            }
            // 2????????????????????????????????????
            if (null != deleteBuffer && !deleteBuffer.isEmpty()) {
                //doOneRecord(deleteBuffer, Constant.DELETEMODE);
                this.doBatchRecordWithPartitionSort(deleteBuffer, Constant.DELETEMODE, bufferSize, batchSize);
                deleteBuffer.clear();
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            writeBuffer.clear();
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    /**
     * @param bufferSize datax??????????????????
     * @param batchSize  datax???ads??????????????????????????????
     * @param buffer     datax?????????
     * @param mode       ???????????????insert ?????? stream
     */
    private void doBatchRecordWithPartitionSort(List<Record> buffer, String mode, int bufferSize, int batchSize) throws SQLException {
        //warn: ?????????????????????????????????, ??????????????????????????????, ?????????????????????????????????, ?????????????????????????????????????????????
        //warn: ???????????????bufferSize????????????batchSize?????????????????????????????????;???????????????????????????????????????batchSize???????????????????????????????????????
        int recordBufferedNumber = buffer.size();
        if (bufferSize > batchSize && recordBufferedNumber > batchSize && this.partitionColumnIndex >= 0) {
            final int partitionColumnIndex = this.partitionColumnIndex;
            final int partitionCount = this.partitionCount;
            Collections.sort(buffer, new Comparator<Record>() {
                @Override
                public int compare(Record record1, Record record2) {
                    int hashPartition1 = AdsInsertProxy.getHashPartition(record1.getColumn(partitionColumnIndex).asString(), partitionCount);
                    int hashPartition2 = AdsInsertProxy.getHashPartition(record2.getColumn(partitionColumnIndex).asString(), partitionCount);
                    return hashPartition1 - hashPartition2;
                }
            });
        }
        // ???????????????Record?????????ads, ??????recordBufferedNumber???
        for (int i = 0; i < recordBufferedNumber; i += batchSize) {
            int toIndex = i + batchSize;
            if (toIndex > recordBufferedNumber) {
                toIndex = recordBufferedNumber;
            }
            this.doBatchRecord(buffer.subList(i, toIndex), mode);
        }
    }

    private void doBatchRecord(final List<Record> buffer, final String mode) throws SQLException {
        List<Class<?>> retryExceptionClasss = new ArrayList<Class<?>>();
        retryExceptionClasss.add(com.mysql.jdbc.exceptions.jdbc4.CommunicationsException.class);
        retryExceptionClasss.add(java.net.SocketException.class);
        try {
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    doBatchRecordDml(buffer, mode);
                    return true;
                }
            }, this.retryTimeUpperLimit, 2000L, true, retryExceptionClasss);
        } catch (SQLException e) {
            LOG.warn(String.format("after retry %s times, doBatchRecord meet a exception: ", this.retryTimeUpperLimit), e);
            LOG.info("try to re execute for each record...");
            doOneRecord(buffer, mode);
            // below is the old way
            // for (Record eachRecord : buffer) {
            // this.taskPluginCollector.collectDirtyRecord(eachRecord, e);
            // }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    //warn: ADS ??????????????????roll back???????????????
    @SuppressWarnings("resource")
    private void doBatchRecordDml(List<Record> buffer, String mode) throws Exception {
        Statement statement = null;
        String sql = null;
        try {
            int bufferSize = buffer.size();
            if (buffer.isEmpty()) {
                return;
            }
            StringBuilder sqlSb = new StringBuilder();
            // connection.setAutoCommit(true);
            //mysql impl warn: if a database access error occurs or this method is called on a closed connection throw SQLException
            statement = this.currentConnection.createStatement();
            sqlSb.append(this.generateDmlSql(this.currentConnection, buffer.get(0), mode));
            for (int i = 1; i < bufferSize; i++) {
                Record record = buffer.get(i);
                this.appendDmlSqlValues(this.currentConnection, record, sqlSb, mode);
            }
            sql = sqlSb.toString();
            if (IS_DEBUG_ENABLE) {
                LOG.debug(sql);
            }
            @SuppressWarnings("unused")
            int status = statement.executeUpdate(sql);
            sql = null;
        } catch (SQLException e) {
            LOG.warn("doBatchRecordDml meet a exception: " + sql, e);
            Exception eachException = e;
            int maxIter = 0;// ???????????????
            while (null != eachException && maxIter < AdsInsertProxy.MAX_EXCEPTION_CAUSE_ITER) {
                if (this.isRetryable(eachException)) {
                    LOG.warn("doBatchRecordDml meet a retry exception: " + e.getMessage());
                    this.currentConnection = AdsUtil.getAdsConnect(this.configuration);
                    throw eachException;
                } else {
                    try {
                        Throwable causeThrowable = eachException.getCause();
                        eachException = causeThrowable == null ? null : (Exception) causeThrowable;
                    } catch (Exception castException) {
                        LOG.warn("doBatchRecordDml meet a no! retry exception: " + e.getMessage());
                        throw e;
                    }
                }
                maxIter++;
            }
            throw e;
        } catch (Exception e) {
            LOG.error("????????????, sql: " + sql);
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(statement, null);
        }
    }

    private void doOneRecord(List<Record> buffer, final String mode) {
        List<Class<?>> retryExceptionClasss = new ArrayList<Class<?>>();
        retryExceptionClasss.add(com.mysql.jdbc.exceptions.jdbc4.CommunicationsException.class);
        retryExceptionClasss.add(java.net.SocketException.class);
        for (final Record record : buffer) {
            try {
                RetryUtil.executeWithRetry(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        doOneRecordDml(record, mode);
                        return true;
                    }
                }, this.retryTimeUpperLimit, 2000L, true, retryExceptionClasss);
            } catch (Exception e) {
                // ???????????????????????????????????????
                this.taskPluginCollector.collectDirtyRecord(record, e);
            }
        }
    }

    @SuppressWarnings("resource")
    private void doOneRecordDml(Record record, String mode) throws Exception {
        Statement statement = null;
        String sql = null;
        try {
            // connection.setAutoCommit(true);
            statement = this.currentConnection.createStatement();
            sql = generateDmlSql(this.currentConnection, record, mode);
            if (IS_DEBUG_ENABLE) {
                LOG.debug(sql);
            }
            @SuppressWarnings("unused")
            int status = statement.executeUpdate(sql);
            sql = null;
        } catch (SQLException e) {
            LOG.error("doOneDml meet a exception: " + sql, e);
            //need retry before record dirty data
            //this.taskPluginCollector.collectDirtyRecord(record, e);
            // ????????????????????????
            Exception eachException = e;
            int maxIter = 0;// ???????????????
            while (null != eachException && maxIter < AdsInsertProxy.MAX_EXCEPTION_CAUSE_ITER) {
                if (this.isRetryable(eachException)) {
                    LOG.warn("doOneDml meet a retry exception: " + e.getMessage());
                    this.currentConnection = AdsUtil.getAdsConnect(this.configuration);
                    throw eachException;
                } else {
                    try {
                        Throwable causeThrowable = eachException.getCause();
                        eachException = causeThrowable == null ? null : (Exception) causeThrowable;
                    } catch (Exception castException) {
                        LOG.warn("doOneDml meet a no! retry exception: " + e.getMessage());
                        throw e;
                    }
                }
                maxIter++;
            }
            throw e;
        } catch (Exception e) {
            LOG.error("????????????, sql: " + sql);
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(statement, null);
        }
    }

    private boolean isRetryable(Throwable e) {
        Class<?> meetExceptionClass = e.getClass();
        if (meetExceptionClass == com.mysql.jdbc.exceptions.jdbc4.CommunicationsException.class) {
            return true;
        }
        if (meetExceptionClass == java.net.SocketException.class) {
            return true;
        }
        return false;
    }

    private String generateDmlSql(Connection connection, Record record, String mode) throws SQLException {
        String sql = null;
        StringBuilder sqlSb = new StringBuilder();
        if (mode.equalsIgnoreCase(Constant.INSERTMODE)) {
            sqlSb.append(this.insertSqlPrefix);
            sqlSb.append("(");
            int columnsSize = this.columns.size();
            for (int i = 0; i < columnsSize; i++) {
                if ((i + 1) != columnsSize) {
                    sqlSb.append("?,");
                } else {
                    sqlSb.append("?");
                }
            }
            sqlSb.append(")");
            //mysql impl warn: if a database access error occurs or this method is called on a closed connection
            PreparedStatement statement = connection.prepareStatement(sqlSb.toString());
            for (int i = 0; i < this.columns.size(); i++) {
                int preparedParamsIndex = i;
                if (Constant.STREAMMODE.equalsIgnoreCase(this.writeMode)) {
                    if (preparedParamsIndex >= this.opColumnIndex) {
                        preparedParamsIndex = i + 1;
                    }
                }
                String columnName = this.columns.get(i);
                int columnSqltype = this.userConfigColumnsMetaData.get(columnName).getLeft();
                prepareColumnTypeValue(statement, columnSqltype, record.getColumn(preparedParamsIndex), i, columnName);
            }
            sql = ((JDBC4PreparedStatement) statement).asSql();
            DBUtil.closeDBResources(statement, null);
        } else {
            sqlSb.append(this.deleteSqlPrefix);
            sqlSb.append("(");
            Set<Entry<String, Integer>> primaryEntrySet = this.primaryKeyNameIndexMap.entrySet();
            int entrySetSize = primaryEntrySet.size();
            int i = 0;
            for (Entry<String, Integer> eachEntry : primaryEntrySet) {
                if ((i + 1) != entrySetSize) {
                    sqlSb.append(String.format(" (%s = ?) and ", eachEntry.getKey()));
                } else {
                    sqlSb.append(String.format(" (%s = ?) ", eachEntry.getKey()));
                }
                i++;
            }
            sqlSb.append(")");
            //mysql impl warn: if a database access error occurs or this method is called on a closed connection
            PreparedStatement statement = connection.prepareStatement(sqlSb.toString());
            i = 0;
            //ads???real time????????????1?????????????????????????????????long, ????????????????????????????????????
            for (Entry<String, Integer> each : primaryEntrySet) {
                String columnName = each.getKey();
                int columnSqlType = this.userConfigColumnsMetaData.get(columnName).getLeft();
                int primaryKeyInUserConfigIndex = this.primaryKeyNameIndexMap.get(columnName);
                if (primaryKeyInUserConfigIndex >= this.opColumnIndex) {
                    primaryKeyInUserConfigIndex++;
                }
                prepareColumnTypeValue(statement, columnSqlType, record.getColumn(primaryKeyInUserConfigIndex), i, columnName);
                i++;
            }
            sql = ((JDBC4PreparedStatement) statement).asSql();
            DBUtil.closeDBResources(statement, null);
        }
        return sql;
    }

    private void appendDmlSqlValues(Connection connection, Record record, StringBuilder sqlSb, String mode) throws SQLException {
        String sqlResult = this.generateDmlSql(connection, record, mode);
        if (mode.equalsIgnoreCase(Constant.INSERTMODE)) {
            sqlSb.append(",");
            sqlSb.append(sqlResult.substring(this.insertSqlPrefix.length()));
        } else {
            // ????????????????????????????????????
            sqlSb.append(" or ");
            sqlSb.append(sqlResult.substring(this.deleteSqlPrefix.length()));
        }
    }

    private void prepareColumnTypeValue(PreparedStatement statement, int columnSqltype, Column column, int preparedPatamIndex, String columnName) throws SQLException {
        java.util.Date utilDate;
        switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                String strValue = column.asString();
                statement.setString(preparedPatamIndex + 1, strValue);
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.REAL:
                String numValue = column.asString();
                if (emptyAsNull && "".equals(numValue) || numValue == null) {
                    //statement.setObject(preparedPatamIndex + 1,  null);
                    statement.setNull(preparedPatamIndex + 1, Types.BIGINT);
                } else {
                    statement.setLong(preparedPatamIndex + 1, column.asLong());
                }
                break;

            case Types.FLOAT:
            case Types.DOUBLE:
                String floatValue = column.asString();
                if (emptyAsNull && "".equals(floatValue) || floatValue == null) {
                    //statement.setObject(preparedPatamIndex + 1,  null);
                    statement.setNull(preparedPatamIndex + 1, Types.DOUBLE);
                } else {
                    statement.setDouble(preparedPatamIndex + 1, column.asDouble());
                }
                break;

            //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
            case Types.TINYINT:
                Long longValue = column.asLong();
                if (null == longValue) {
                    statement.setNull(preparedPatamIndex + 1, Types.BIGINT);
                } else {
                    statement.setLong(preparedPatamIndex + 1, longValue);
                }

                break;

            case Types.DATE:
                java.sql.Date sqlDate = null;
                try {
                    if ("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "Date ?????????????????????[%s]", column));
                }

                if (null != utilDate) {
                    sqlDate = new java.sql.Date(utilDate.getTime());
                }
                statement.setDate(preparedPatamIndex + 1, sqlDate);
                break;

            case Types.TIME:
                java.sql.Time sqlTime = null;
                try {
                    if ("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIME ?????????????????????[%s]", column));
                }

                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                statement.setTime(preparedPatamIndex + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                try {
                    if ("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIMESTAMP ?????????????????????[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(
                            utilDate.getTime());
                }
                statement.setTimestamp(preparedPatamIndex + 1, sqlTimestamp);
                break;

            case Types.BOOLEAN:
                //case Types.BIT: ads ??????bit
                Boolean booleanValue = column.asBoolean();
                if (null == booleanValue) {
                    statement.setNull(preparedPatamIndex + 1, Types.BOOLEAN);
                } else {
                    statement.setBoolean(preparedPatamIndex + 1, booleanValue);
                }

                break;
            default:
                Pair<Integer, String> columnMetaPair = this.userConfigColumnsMetaData.get(columnName);
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "?????????????????????????????????????????????. ??????DataX ??????????????????????????????????????????. ?????????:[%s], ????????????:[%s], ??????Java??????:[%s]. ?????????????????????????????????????????????????????????.",
                                        columnName, columnMetaPair.getRight(), columnMetaPair.getLeft()));
        }
    }

    private static int getHashPartition(String value, int totalHashPartitionNum) {
        long crc32 = (value == null ? getCRC32("-1") : getCRC32(value));
        return (int) (crc32 % totalHashPartitionNum);
    }

    private static long getCRC32(String value) {
        Checksum checksum = new CRC32();
        byte[] bytes = value.getBytes();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }

    @Override
    public void closeResource() {
    }
}
