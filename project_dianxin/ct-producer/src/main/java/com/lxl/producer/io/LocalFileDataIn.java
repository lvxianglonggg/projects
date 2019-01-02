package com.lxl.producer.io;

import com.lxl.common.bean.Data;
import com.lxl.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LocalFileDataIn implements DataIn {

    private BufferedReader reader;


    public LocalFileDataIn(String path){
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


    }

    public void close() throws IOException {
        if (null != reader) {
            reader.close();
        }
    }

    public <T extends Data> List<T> read(Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<T>();
        String line = null;
        while (null != (line = reader.readLine())) {
            T t = clazz.newInstance();
            t.setContect(line);
            list.add(t);
        }
        return list;
    }
}
