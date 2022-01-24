package org.tianyc.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.base.BaseObject;

import java.io.Serializable;

/**
 * @author ：Wangtao
 * @date ：Created in 2021/1/17 3:29 下午
 * @description：kafkaTopic封装类
 */

public class KafkaTopic extends BaseObject implements Serializable {
    private String name;
    private int partation;
//    private int maxPartation;

    public String getName() {
        return name;
    }

    public int getPartation() {
        return partation;
//       if(partation>0){
//           return partation;
//       }else{
//           return new Random().nextInt(maxPartation);
//       }
    }

//    public int getMaxPartation() {
//        return maxPartation;
//    }

    private KafkaTopic(Builder builder){
        this.name = builder.name;
        this.partation = builder.partation;
//        this.maxPartation = builder.maxPartation;

    }


    public static class Builder{
        private String name;
        private int partation;
//        private int maxPartation;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setPartation(int partation) {
            this.partation = partation;
            return this;
        }

//        public Builder setMaxPartation(int maxPartation) {
////            this.maxPartation = maxPartation;
//            return this;
//        }

        public KafkaTopic build(){
            return new KafkaTopic(this);
        }

    }


}
