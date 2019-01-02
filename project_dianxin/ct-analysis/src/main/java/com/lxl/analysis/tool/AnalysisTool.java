package com.lxl.analysis.tool;

import com.lxl.analysis.bean.AnalysisKey;
import com.lxl.analysis.bean.AnalysisValue;
import com.lxl.analysis.io.AnalysisOutputFormat;
import com.lxl.analysis.mapper.AnalysisMapper;
import com.lxl.analysis.reducer.AnalysisReducer;
import com.lxl.common.constant.NameConst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class AnalysisTool implements Tool {

    private Configuration conf;

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(AnalysisTool.class);
        // mapper
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(NameConst.COLUMN_FAMILY_CALLER));

        TableMapReduceUtil.initTableMapperJob(NameConst.TABLE,
                                            scan,
                                            AnalysisMapper.class,
                                             AnalysisKey.class,
                                            Text.class,
                                            job);

        // reducer
        job.setReducerClass(AnalysisReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        // outputformat
        job.setOutputFormatClass(AnalysisOutputFormat.class);

        boolean b = job.waitForCompletion(true);
        return b ?  JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
}
