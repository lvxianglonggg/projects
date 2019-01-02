package com.lxl.consume.dao;

import com.lxl.common.api.ColumnRef;
import com.lxl.common.api.RowkeyRef;
import com.lxl.common.api.TableRef;
import com.lxl.common.constant.NameConst;
import com.lxl.common.constant.ValueConst;
import com.lxl.common.dao.BaseDao;
import com.lxl.consume.bean.Calllog;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;

public class HbaseDao extends BaseDao {

    public void init() throws Exception {
        start();
        createNamespaceNX(NameConst.NAMESPACE);
        createTableXX(NameConst.TABLE, "com.lxl.consume.coprocessor.InsertCalleeCoprocessor", NameConst.COLUMN_FAMILY_CALLER, NameConst.COLUMN_FAMILY_CALLEE);
        end();
    }

    public void scanData(String tel, String start, String end) throws Exception {
        start();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(""));
        scan.setStopRow(Bytes.toBytes(""));

        Table table = null;
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {

        }
    }

    /**
     * 更普遍性
     *
     * @param obj
     * @throws Exception
     */
    public void put(Object obj) throws Exception {
        start();
        Class<?> clazz = obj.getClass();
        // 取表名
        TableRef tableRef = clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();
        // 取属性rowkey
        String rowkey = "";
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            RowkeyRef rowkeyRef = field.getAnnotation(RowkeyRef.class);
            if (rowkeyRef != null) {// 注解不为空时，找到rowkey注解
                //先将私有属性设计为可见
                field.setAccessible(true);
                rowkey = (String) field.get(obj);
                break;
            }
        }
        // 生成put对象，设置列族，列信息
        Put put = new Put(Bytes.toBytes(rowkey));
        for (Field field : fields) {
            ColumnRef columnRef = field.getAnnotation(ColumnRef.class);
            if (null != columnRef) {
                String fimily = columnRef.fimily();
                String column = columnRef.column();
                if (column == null || "".equals(column)) {
                    column = field.getName();
                }
                field.setAccessible(true);
                String value = (String) field.get(obj);
                put.addColumn(Bytes.toBytes(fimily), Bytes.toBytes(column), Bytes.toBytes(value));
            }
        }
        putData(tableName, put);
        end();
    }

    /**
     * 存放数据
     * rowkey设计原则，
     * 1.惟一性原则，主键
     * 2.长度原则，64KB,推荐10~100byte,一般60-80
     * 一般是8的位数，在满足查询的情况下，越短越好
     * 3.散列原则，希望将rowkey所代表的数据均匀分布到不同分区中
     * 4.满足业务要求
     *
     * @param data
     * @throws Exception
     */
    public void put(String data) throws Exception {
        start();
        String[] fields = data.split("\t");
        String call1 = fields[0];
        String call2 = fields[1];
        String calltime = fields[2];
        String duration = fields[3];
        String regionNum = genRegionNum(call1, calltime, ValueConst.REGION_DEFAULT_COUNT);
        String rowkey = regionNum + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration;

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_INFO), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_INFO), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_INFO), Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        put.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_INFO), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        putData(NameConst.TABLE, put);

        end();
    }

    public void genRowkey(Calllog calllog, String flag) {
        String rowkey = genRegionNum(calllog.getCall1(), calllog.getCalltime().substring(0, 6), ValueConst.REGION_DEFAULT_COUNT) + "_" + calllog.getCall1() + "_" + calllog.getCalltime()
                + "_" + calllog.getCall2() + "_" + calllog.getDuration() + "_" + flag;
        calllog.setRowkey(rowkey);
    }
}
