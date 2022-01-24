package org.tianyc.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.base.BaseObject;

import java.io.Serializable;

/**
 * @author ：Wangtao
 * @date ：Created in 2021/1/17 3:29 下午
 * @description：数据库列的封装类
 */

public class KafkaColumnCell extends BaseObject implements Serializable{
    private String name;
    private ColumnType type;
    private String dateFormat;
    private String value;
    private String fieldType;

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getValue() {
        return value;
    }

    public String getFieldType() {
        return fieldType;
    }

    private KafkaColumnCell(Builder builder){
        this.dateFormat = builder.dateFormat;
        this.name = builder.name;
        this.type = builder.type;
        this.value = builder.value;
        this.fieldType = builder.fieldType;
    }

    public static class Builder{
        private String name;
        private ColumnType type;
        private String dateFormat;
        private String value;
        private String fieldType;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setType(ColumnType type) {
            this.type = type;
            return this;
        }

        public Builder setDateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }

        public Builder setValue(String value) {
            this.value = value;
            return this;
        }

        public Builder setFieldType(String fieldType) {
            this.fieldType = fieldType;
            return this;
        }



        public KafkaColumnCell build(){
            return new KafkaColumnCell(this);
        }
    }


}
