package com.lxl.common.constant;

public enum FormatEnum {

    DATE_YMDHMS("yyyyMMddHHmmss");

    private FormatEnum(String f) {
        format = f;
    }

    private String format;

    public String getFormat() {
        return format;
    }
}
