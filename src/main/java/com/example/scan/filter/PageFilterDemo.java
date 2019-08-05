package com.example.scan.filter;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class PageFilterDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        Scan scan = new Scan(Bytes.toBytes("my-row-key0"), Bytes.toBytes(""));
        PageFilter pageFilter = new PageFilter(2); //表示返回两个rowKey的数据
        scan.setFilter(pageFilter);

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
