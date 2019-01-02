package com.lxl.common.bean;

import java.io.Closeable;

public interface MyConsumer extends Closeable {

    void consume();
}
