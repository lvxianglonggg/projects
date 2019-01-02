package com.lxl.consume.bean;

import com.lxl.common.bean.MyConsumer;
import com.lxl.common.constant.ValueConst;
import com.lxl.consume.dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Flume2KafkaComsumer implements MyConsumer {

    public void consume() {
        try {
            Properties properties = new Properties();
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList("calllog"));
            HbaseDao dao = new HbaseDao();
            dao.init();
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    // dao.put(record.value());
                    String[] values = record.value().split("\t");
                    Calllog calllog = new Calllog(values[0],values[1],values[2],values[3]);
                    dao.genRowkey(calllog, ValueConst.CALLLOG_FLAG_CALLER);
                    System.out.println("产生的rowkey:" + calllog.getRowkey());
                    dao.put(calllog);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
    }
}
