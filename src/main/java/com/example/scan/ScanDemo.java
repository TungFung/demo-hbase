package com.example.scan;

import com.example.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class ScanDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        /** endRow是exclusive的,如果留空会把剩下的全查 */
        Scan scan = new Scan(Bytes.toBytes("my-row-key0"), Bytes.toBytes(""));
        //scan.addColumn(Bytes.toBytes("f1"), null);

        //batch和caching和hbase table column size共同决意了rpc的次数
        //详见 https://www.cnblogs.com/seaspring/p/6861957.html
        scan.setCaching(-1);//每次rpc的请求记录数，默认是1；cache大可以优化性能，但是太大了会花费很长的时间进行一次传输。
        scan.setBatch(-1);//设置每次取的column size；有些row特别大，所以需要分开传给client，就是一次传一个row的几个column。

        //scan.setMaxResultSize(3L);

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
