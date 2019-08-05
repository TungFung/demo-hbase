package com.example.dml;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;

public class GetDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        /** 查单个rowKey的记录 */
        Get get = new Get(Bytes.toBytes("my-row-key0"));
        get.addColumn(Bytes.toBytes("f1"), null);//添加column的筛选条件
        get.setMaxVersions(3);//最近三个版本

        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        Utils.printCellsInfo(cells);

        /** 查多个rowKey的记录 */
        List<Get> gets = new LinkedList<>();
        Get get0 = new Get(Bytes.toBytes("my-row-key0"));
        Get get1 = new Get(Bytes.toBytes("my-row-key1"));
        Get get2 = new Get(Bytes.toBytes("my-row-key2"));
        gets.add(get0);
        gets.add(get1);
        gets.add(get2);

        Result[] results = table.get(gets);
        for(Result r: results){
            List<Cell> cellList = r.listCells();
            Utils.printCellsInfo(cellList);
        }

        connection.close();
    }


}
