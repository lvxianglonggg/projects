package com.lxl.common.bean;

import java.io.Closeable;
import java.util.List;

/**
 * 数据输入
 */
public interface DataIn extends Closeable{

    public <T extends Data> List<T> read(Class<T> clazz) throws Exception;
}
