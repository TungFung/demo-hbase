package com.example.dml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;

public class DeleteDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        Delete delete = new Delete(Bytes.toBytes("my-row-key0"));
        table.delete(delete);

        List<Delete> deletes = new LinkedList<>();
        Delete delete1 = new Delete(Bytes.toBytes("my-row-key1"));
        Delete delete2 = new Delete(Bytes.toBytes("my-row-key2"));
        deletes.add(delete1);
        deletes.add(delete2);
        table.delete(deletes);

        connection.close();
    }
}
