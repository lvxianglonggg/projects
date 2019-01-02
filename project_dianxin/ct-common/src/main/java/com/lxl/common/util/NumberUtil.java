package com.lxl.common.util;

import java.text.DecimalFormat;

public class NumberUtil {

    public static String format(int num, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("0");
        }
        DecimalFormat df = new DecimalFormat(sb.toString());
        return df.format(num);

    }

    public static void main(String[] args) {
        String s = format(22, 6);
        System.out.println(s);
    }
}
