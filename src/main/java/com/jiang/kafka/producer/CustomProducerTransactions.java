package com.jiang.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 事务
 * @author jiang
 * @create 2022-07-02-11:27 上午
 */
public class CustomProducerTransactions {

    public static void main(String[] args) {
        //0 配置
        Properties properties = new Properties();
        //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        //指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定事务id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactional_id_01");

        //1、创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //初始化事务
        kafkaProducer.initTransactions();
        //启动事务
        kafkaProducer.beginTransaction();

        try{
            //2、发送数据
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("first","atguigu"+i));
            }
            int i=1/0;
            //提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            //中止事务
            kafkaProducer.abortTransaction();
        }finally {
            //3、关闭资源
            kafkaProducer.close();
        }



    }
}
