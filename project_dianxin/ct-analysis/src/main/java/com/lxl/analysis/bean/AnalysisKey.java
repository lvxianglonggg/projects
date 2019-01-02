package com.lxl.analysis.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String time;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel, String time) {
        this.tel = tel;
        this.time = time;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int compareTo(AnalysisKey o) {
        int result = this.tel.compareTo(o.tel);
        if (result == 0) {
            result =  time.compareTo(o.time);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeUTF(time);
    }

    public void readFields(DataInput in) throws IOException {
        tel = in.readUTF();
        time = in.readUTF();
    }

    @Override
    public String toString() {
        return "AnalysisKey{" +
                "tel='" + tel + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
