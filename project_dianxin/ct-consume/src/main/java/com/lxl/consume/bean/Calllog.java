package com.lxl.consume.bean;

import com.lxl.common.api.ColumnRef;
import com.lxl.common.api.RowkeyRef;
import com.lxl.common.api.TableRef;

@TableRef("atguigu:calllog")
public class Calllog {

    public Calllog() {
    }

    public Calllog(String call1, String call2, String calltime, String duration) {
        this.call1 = call1;
        this.call2 = call2;
        this.calltime = calltime;
        this.duration = duration;
    }

    @RowkeyRef
    private String rowkey;
    @ColumnRef()
    private String call1;
    @ColumnRef
    private String call2;
    @ColumnRef
    private String calltime;
    @ColumnRef
    private String duration;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }
}
