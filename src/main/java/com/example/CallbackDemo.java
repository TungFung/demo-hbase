package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;

public class CallbackDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        List<Put> list = new LinkedList<Put>();
        for(int i = 15; i < 20; i++){
            Put p = new Put(Bytes.toBytes("my-row-key" + i));
            p.addColumn(Bytes.toBytes("f1"), null, Bytes.toBytes("my-f1-value-" + i));
            p.addColumn(Bytes.toBytes("f2"), null, Bytes.toBytes("my-f2-value-" + i));
            list.add(p);
        }
        table.put(list);

        //返回结果信息
        Object[] results = new Object[list.size()];

        //操作回调
        table.batchCallback(list, results, new Batch.Callback<Result>() {
            @Override
            public void update(byte[] region, byte[] row, Result result) {
                System.out.println(Bytes.toString(region));//region信息
                System.out.println(Bytes.toString(row));//rowKey信息
                System.out.println(result);
            }
        });
        //System.out.println(results);
        connection.close();
    }
}
