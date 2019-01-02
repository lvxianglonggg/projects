package com.lxl.common.bean;

import java.io.Closeable;

public interface DataOut extends Closeable {

    public void write();

    public void write(String line);

    public void write(Object obj);
}
