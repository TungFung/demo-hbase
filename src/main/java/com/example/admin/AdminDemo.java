package com.example.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class AdminDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        /** 创建namespace */
//        admin.createNamespace(NamespaceDescriptor.create("twq").build());
//        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
//        for(NamespaceDescriptor namespaceDescriptor: namespaceDescriptors){
//            System.out.println(namespaceDescriptor.getName());
//        }

        /** 创建table 建表至少要有一个column family，而且加上命名空间，否则命名空间到default */
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("twq:test"));
        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));
        admin.createTable(hTableDescriptor);

        /** 删除table */
//        admin.disableTable(TableName.valueOf("twq:test"));
//        admin.deleteTable(TableName.valueOf("twq:test"));

        /** 查询 */
//        TableName[] tableNames = admin.listTableNames();
//        for(TableName tableName: tableNames){
//            System.out.println(tableName.getNameAsString());
//
//            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName);
//            HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
//            for(HColumnDescriptor hColumnDescriptor: hColumnDescriptors){
//                System.out.println(hColumnDescriptor.getNameAsString());
//            }
//        }



        connection.close();
    }
}
