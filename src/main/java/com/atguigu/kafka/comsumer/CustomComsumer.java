package com.atguigu.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class CustomComsumer {
    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "0218");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000);


        //1.创建KafkaConsumer对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //2.调用subscribe()方法
        consumer.subscribe(Arrays.asList("first"));


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.topic() + "-" + record.partition() + "-" + record.offset() + ":" + record.value());

                }
            }
        } finally {
            consumer.close();
        }
    }
}
