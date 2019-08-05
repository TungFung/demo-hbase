package com.example.dml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;

/**
 * hbase的写入操作和更新操作都是Put，如果数据已经存在会修改，否则新增
 */
public class PutDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        /** 单次写入 */
        Put put = new Put(Bytes.toBytes("my-row-key"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("i"), Bytes.toBytes(0L));
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c"), Bytes.toBytes("c-value"));
        table.put(put);

        /** 批量写入 */
//        List<Put> list = new LinkedList<Put>();
//        for(int i = 0; i < 10; i++){
//            Put p = new Put(Bytes.toBytes("my-row-key" + i));
//            p.addColumn(Bytes.toBytes("f1"), null, Bytes.toBytes("my-f1-value-" + i));
//            p.addColumn(Bytes.toBytes("f2"), null, Bytes.toBytes("my-f2-value-" + i));
//            list.add(p);
//        }
//        table.put(list);

        connection.close();
    }
}
