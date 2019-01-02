package com.lxl.consume.coprocessor;

import com.lxl.common.constant.NameConst;
import com.lxl.common.constant.ValueConst;
import com.lxl.common.dao.BaseDao;
import com.lxl.common.util.DateUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class InsertCalleeCoprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String callerRowkey = Bytes.toString(put.getRow());
        // 5_19154926260_20181008012039_17405139883_0535_1
        System.out.println("callerRowkey=" + callerRowkey);
        String[] values = callerRowkey.split("_");
        String flag = values[5];
        String call1 = values[3];
        String calltime = values[2];
        String call2 = values[1];
        String duration = values[4];

        if (ValueConst.CALLLOG_FLAG_CALLER.equals(flag)) {
            CoprocessorDao dao = new CoprocessorDao();
            String rowkey = dao.getRegionNum(call1, calltime.substring(0, 6), ValueConst.REGION_DEFAULT_COUNT) + "_" + call1
                    + "_" + calltime + "_" + call2 + "_" + duration + "_" + ValueConst.CALLLOG_FLAG_CALLEE;
            Put calleePut = new Put(Bytes.toBytes(rowkey));
            calleePut.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_CALLEE), Bytes.toBytes("call1"), Bytes.toBytes(call1));
            calleePut.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_CALLEE), Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_CALLEE), Bytes.toBytes("call2"), Bytes.toBytes(call2));
            calleePut.addColumn(Bytes.toBytes(NameConst.COLUMN_FAMILY_CALLEE), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            // 增加被叫数据
            TableName tableName = TableName.valueOf(NameConst.TABLE);
            Table table = e.getEnvironment().getTable(tableName);
            table.put(calleePut);
            table.close();
        }
    }

    private class CoprocessorDao extends BaseDao {
        public String getRegionNum(String tel, String date, int reginCount) {
            return genRegionNum(tel, date, reginCount);
        }
    }
}
