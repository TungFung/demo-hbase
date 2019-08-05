package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * 异步操作
 */
public class BufferedMutatorDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf("twq:test"));

        //异步写
        List<Put> puts = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("mutate-row-key"));
        put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("m1"), Bytes.toBytes("mutate-a-value"));
        put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("m2"), Bytes.toBytes("mutate-b-value"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("mutate-row-key"));
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("m3"), Bytes.toBytes("mutate-c-value"));
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("m4"), Bytes.toBytes("mutate-d-value"));
        puts.add(put2);

        bufferedMutator.mutate(puts);
        bufferedMutator.flush();
        bufferedMutator.close();
        connection.close();
    }
}
