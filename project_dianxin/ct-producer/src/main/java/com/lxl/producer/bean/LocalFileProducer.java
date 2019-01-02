package com.lxl.producer.bean;

import com.lxl.common.bean.DataIn;
import com.lxl.common.bean.DataOut;
import com.lxl.common.bean.Producer;
import com.lxl.common.constant.FormatConst;
import com.lxl.common.constant.FormatEnum;
import com.lxl.common.util.DateUtil;
import com.lxl.common.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class LocalFileProducer implements Producer {

    private DataIn dataIn;
    private DataOut dataOut;
    private volatile boolean flag = true; // 增强内存可见性

    public void setIn(DataIn in) {
        dataIn = in;
    }

    public void setOut(DataOut out) {
        dataOut = out;
    }

    public void produce() {
        try {
            List<Contact> contacts = dataIn.read(Contact.class);
            dataIn.close();

            while (flag) {
                // 1.随机生成两个通话用户
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true){
                    call2Index = new Random().nextInt(contacts.size());
                    if (call2Index != call1Index){
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                // 2.随机生成通话时间
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime = DateUtil.parse(startDate, FormatConst.DATE_YMDHMS).getTime();
                long endTime = DateUtil.parse(endDate, FormatEnum.DATE_YMDHMS.getFormat()).getTime();

                long calltime = startTime + (long)((endTime - startTime)*Math.random());
                String callTimeStr = DateUtil.format(new Date(calltime),FormatConst.DATE_YMDHMS);


                // 3 随机生成通话时长
                String duration = NumberUtil.format(new Random().nextInt(1800),4);
                CallLog callLog = new CallLog(call1.getTel(), call2.getTel(),callTimeStr,duration);

                System.out.println(callLog);
                // 4 将通话信息输出到指定的文件中
                dataOut.write(callLog);

                // 5 每秒钟生成两条数据
                Thread.sleep(500);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        if (null != dataIn) {
            dataIn.close();
        }
        if (null != dataOut) {
            dataOut.close();
        }

    }
}
