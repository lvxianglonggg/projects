package com.lxl.producer.bean;

import com.lxl.common.bean.Data;

public class Contact extends Data {

    private String name;
    private String tel;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    @Override
    public void setContect(String contect) {
        String[] values = contect.split("\t");
        setTel(values[0]);
        setName(values[1]);
    }
}
