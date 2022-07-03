package com.jiang.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jiang
 * @create 2022-07-02-11:27 上午
 */
public class CustomProducerCallback {

    public static void main(String[] args) {
        //0 配置
        Properties properties = new Properties();
        //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        //指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //1、创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2、发送数据
        try{
            for (int i = 0; i < 500; i++) {
                kafkaProducer.send(new ProducerRecord<>("first","atguigu"+i),(metadata,exception)->{
                    if(exception==null){
                        System.out.println("主题："+metadata.topic()+" 分区："+metadata.partition());
                    }
                });
                Thread.sleep(2L);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //3、关闭资源
            kafkaProducer.close();
        }

    }
}
