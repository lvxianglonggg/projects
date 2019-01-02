package com.lxl.analysis.mapper;

import com.lxl.analysis.bean.AnalysisKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AnalysisMapper extends TableMapper<AnalysisKey, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        System.out.println("我进mapper方法了。~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        String rowkey = Bytes.toString(key.get());
        String[] values = rowkey.split("_");

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flag = values[5];


        String year = calltime.substring(0,4);
        String month = calltime.substring(0,6);
        String day = calltime.substring(0,8);

        context.write(new AnalysisKey(call1,year),new Text(duration));
        context.write(new AnalysisKey(call1,month),new Text(duration));
        context.write(new AnalysisKey(call1,day),new Text(duration));

        context.write(new AnalysisKey(call2,year),new Text(duration));
        context.write(new AnalysisKey(call2,month),new Text(duration));
        context.write(new AnalysisKey(call2,day),new Text(duration));
    }
}
