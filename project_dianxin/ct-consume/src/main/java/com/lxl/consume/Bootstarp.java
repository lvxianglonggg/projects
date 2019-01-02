package com.lxl.consume;

import com.lxl.consume.bean.Flume2KafkaComsumer;

public class Bootstarp {

    public static void main(String[] args) {
        Flume2KafkaComsumer comsumer = new Flume2KafkaComsumer();
        comsumer.consume();
    }
}
