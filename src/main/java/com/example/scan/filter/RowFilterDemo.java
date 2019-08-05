package com.example.scan.filter;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class RowFilterDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        /** endRow是exclusive的,如果留空会把剩下的全查 */
        Scan scan = new Scan(Bytes.toBytes("my-row-key0"), Bytes.toBytes(""));

        //1、查询出rowkey大于等于my-row-key3的数据
//        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("key3"));
//        RowFilter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, binaryComparator);

        //2、查询出rowkey以3结尾的数据记录
//        RegexStringComparator regexStringComparator = new RegexStringComparator("3");
//        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator);

        //3、查询出rowkey包含字符串"128"的数据
        SubstringComparator substringComparator = new SubstringComparator("key3");
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
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
