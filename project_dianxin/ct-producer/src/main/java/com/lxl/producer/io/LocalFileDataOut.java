package com.lxl.producer.io;

import com.lxl.common.bean.DataOut;

import java.io.*;

public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write() {
        writer.println();
    }

    public void write(String line) {
        writer.println(line);
        writer.flush();
    }

    public void write(Object obj) {
        write(obj.toString());
    }

    public void close() throws IOException {
        if (null != writer) {
            writer.close();
        }
    }
}
