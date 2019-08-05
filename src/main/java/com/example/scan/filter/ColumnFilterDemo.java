package com.example.scan.filter;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class ColumnFilterDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        Scan scan = new Scan(Bytes.toBytes("my-row-key"), Bytes.toBytes(""));

        //1、查询column family等于f1的数据,
        //FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1")));
        //scan.setFilter(filter);
        //上面的还不如使用
        //scan.addFamily(Bytes.toBytes("f1"));

        //2、查询某一行中的某一个column family中的column qualifier等于a的数据
        //QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("a")));

        //3、查询某一行中的某一个column family中的column qualifier的前缀是a的数据
        //ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("a"));

        //4、查询某一行中的某一个column family中的column qualifier的前缀是abc或者c的数据
        byte[][] prefixes = new byte[][] {Bytes.toBytes("abc"), Bytes.toBytes("c")};
        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefixes);

        //5、查询某一行中的某一个column family中的column qualifier从a到b的数据
        //ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true);

        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        Result result = resultScanner.next();
        while(result != null){
            List<Cell> cells = result.listCells();
            Utils.printCellsInfo(cells);
            result = resultScanner.next();
        }

        connection.close();
    }
}
