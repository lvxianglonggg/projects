package com.lxl.analysis.io;

import com.lxl.analysis.bean.AnalysisKey;
import com.lxl.analysis.bean.AnalysisValue;
import com.lxl.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AnalysisOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    private class MysqlRecordWriter extends RecordWriter<AnalysisKey,AnalysisValue> {
        private Connection conn;
        public MysqlRecordWriter(){
            conn = JDBCUtil.getConnection();
        }

        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {
            String insertSql = "insert into t_calllog(tel,calltime,sumcount,sumduration) values(?,?,?,?)";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = conn.prepareStatement(insertSql);
                preparedStatement.setString(1,key.getTel());
                preparedStatement.setString(2,key.getTime());
                preparedStatement.setInt(3,value.getSumcount());
                preparedStatement.setInt(4,value.getSumduration());
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (null != preparedStatement) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MysqlRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }

    private FileOutputCommitter committer = null;

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}
