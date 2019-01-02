package com.lxl.common.dao;

import com.lxl.common.constant.FormatConst;
import com.lxl.common.constant.ValueConst;
import com.lxl.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public abstract class BaseDao {

    ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() {
        try {
            Connection connection = connHolder.get();
            if (null == connection) {
                Configuration conf = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(conf);
                connHolder.set(connection);
            }
            Admin admin = adminHolder.get();
            if (null == admin) {
                admin = connection.getAdmin();
                adminHolder.set(admin);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    protected void end() {
        try {
            Admin admin = adminHolder.get();
            if (null != admin) {
                admin.close();
                adminHolder.remove();
            }
            Connection connection = connHolder.get();
            if (null != connection) {
                connection.close();
                connHolder.remove();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void createNamespaceNX(String nameSpace) throws Exception {
        Admin admin = adminHolder.get();
        try {
            admin.getNamespaceDescriptor(nameSpace);
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 生成分区号，记数据放到指定分区中
     */
    protected String genRegionNum(String tel, String time, int reginCount) {

        // 散列
        int telHashCode = tel.hashCode();
        int timeHashCode = time.hashCode();

        // 取模
        int hash = Math.abs(telHashCode ^ timeHashCode);

        // 计算分区号
        int regionNum = hash % reginCount;
        return regionNum + "";
    }

    protected void createTableXX(String tableName, String coprocessorClassName, String... columnFamilies) throws Exception {
        createTableXX(tableName, coprocessorClassName, ValueConst.REGION_DEFAULT_COUNT, columnFamilies);
    }

    protected void createTableXX(String tableName, String clazzName, int regionCount, String... columnFamilies) throws Exception {
        Admin admin = adminHolder.get();
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(table);
        if (null != columnFamilies && columnFamilies.length != 0) {
            for (String columnFamily : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
        }

        // 增加协处理器
        if (null != clazzName && !"".equals(clazzName)) {
            tableDescriptor.addCoprocessor(clazzName);
        }

        // 使用分区键创建表(预分区)
        admin.createTable(tableDescriptor, genSplitKeys(regionCount));
    }

    private byte[][] genSplitKeys(int regionCount) {
        int splitKeySize = regionCount - 1;
        byte[][] keys = new byte[splitKeySize][];
        List<byte[]> splitKeyList = new ArrayList<byte[]>();
        // Collections.sort(splitKeyList);
        for (int i = 0; i < splitKeySize; i++) {
            splitKeyList.add(Bytes.toBytes(i + "|"));
        }
        splitKeyList.toArray(keys);
        return keys;
    }

    protected void putData(String tableName, Put put) throws Exception {
        Connection connection = connHolder.get();
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
    }

    /**
     * @param tel
     * @param start 201806
     * @param end
     * @return
     */
    public List<String[]> getStartStopRowkeys(String tel, String start, String end) {
        List<String[]> rowkeys = new ArrayList<String[]>();
        try {
            Calendar startDate = Calendar.getInstance();
            startDate.setTime(DateUtil.parse(start, FormatConst.DATE_YM));

            Calendar endDate = Calendar.getInstance();
            endDate.setTime(DateUtil.parse(end, FormatConst.DATE_YM));
            while (startDate.getTimeInMillis() <= endDate.getTimeInMillis()) {

                String startRowkey = DateUtil.format(startDate.getTime(), FormatConst.DATE_YM);
                String regionNum = genRegionNum(tel, startRowkey, ValueConst.REGION_DEFAULT_COUNT);
                startRowkey = regionNum + "_" + tel + "_" + startRowkey;
                String stopRowkey = startRowkey + "|";
                String[] rowkeyRange = {startRowkey, stopRowkey};
                rowkeys.add(rowkeyRange);
                startDate.add(Calendar.MONTH, 1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return rowkeys;
    }

}
