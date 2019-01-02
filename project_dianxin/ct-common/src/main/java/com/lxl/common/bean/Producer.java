package com.lxl.common.bean;

import java.io.Closeable;

public interface Producer extends Closeable {

    void setIn(DataIn in);

    void setOut(DataOut out);


    /**
     * 生产数据
     */
    void produce();
}
