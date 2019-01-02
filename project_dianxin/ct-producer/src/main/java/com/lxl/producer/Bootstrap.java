package com.lxl.producer;

import com.lxl.common.bean.Producer;
import com.lxl.producer.bean.LocalFileProducer;
import com.lxl.producer.io.LocalFileDataIn;
import com.lxl.producer.io.LocalFileDataOut;

import java.io.IOException;

public class Bootstrap {

    public static void main(String[] args) throws IOException {
        if (args.length != 2){
            System.out.println("请输入合适的2个参数！");
            return;
        }

        Producer producer = new LocalFileProducer();
        LocalFileDataIn dataIn = new LocalFileDataIn(args[0]);
        LocalFileDataOut dataOut = new LocalFileDataOut(args[1]);

        // LocalFileDataIn dataIn = new LocalFileDataIn("F:\\大数据资料\\课件包\\13_尚硅谷大数据技术之电信客服\\2.资料\\辅助文档\\contact.log");
        // LocalFileDataOut dataOut = new LocalFileDataOut("F:\\大数据资料\\课件包\\13_尚硅谷大数据技术之电信客服\\2.资料\\辅助文档\\call.log");
        producer.setIn(dataIn);
        producer.setOut(dataOut);
        // 生产数据
        producer.produce();
        // 释放资源
        producer.close();

    }
}
