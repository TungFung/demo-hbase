package com.example.scan.filter;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class ValuFilterDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        Scan scan = new Scan(Bytes.toBytes("my-row-key"), Bytes.toBytes(""));

        //单个值
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("a-value")));
        scan.setFilter(filter);

        //多个值
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        ValueFilter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("c-value")));
        ValueFilter filter2 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("b-value")));
        list.addFilter(filter1);
        list.addFilter(filter2);
        scan.setFilter(list);

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
