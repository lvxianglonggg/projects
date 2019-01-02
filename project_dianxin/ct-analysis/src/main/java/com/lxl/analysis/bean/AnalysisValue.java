package com.lxl.analysis.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisValue implements Writable {

    private int sumcount;
    private int sumduration;

    public AnalysisValue() {
    }

    public AnalysisValue(int sumcount, int sumduration) {
        this.sumcount = sumcount;
        this.sumduration = sumduration;
    }

    public int getSumcount() {
        return sumcount;
    }

    public void setSumcount(int sumcount) {
        this.sumcount = sumcount;
    }

    public int getSumduration() {
        return sumduration;
    }

    public void setSumduration(int sumduration) {
        this.sumduration = sumduration;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.sumcount + "");
        out.writeUTF(this.sumduration + "");
    }

    public void readFields(DataInput in) throws IOException {
        sumcount = Integer.parseInt(in.readUTF());
        sumduration = Integer.parseInt(in.readUTF());
    }
}
