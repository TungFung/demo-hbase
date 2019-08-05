package com.example.dml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class AtomicDmlDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("twq:test"));

        //给某一列的数值加1，这一列的value需要是long类型的
        Increment increment = new Increment(Bytes.toBytes("my-row-key"));
        increment.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("i"), 1);
        table.increment(increment);

        //给value追加内容
//        Append append = new Append(Bytes.toBytes("my-row-key0"));
//        append.add(Bytes.toBytes("f1"), null, Bytes.toBytes("test-append"));
//        table.append(append);

        //如果存在给定rowKey,family,qualifier,value的记录，那么执行put
//        Put put = new Put(Bytes.toBytes("my-row-key"));
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("a"), Bytes.toBytes("aaa-value"));
//        table.checkAndPut(Bytes.toBytes("my-row-key"), Bytes.toBytes("f1"), Bytes.toBytes("a"), Bytes.toBytes("a-value"), put);

        //如果存在给定rowKey,family,qualifier,value的记录，那么执行delete
//        Delete delete = new Delete(Bytes.toBytes("my-row-key"));
//        table.checkAndDelete(Bytes.toBytes("my-row-key"), Bytes.toBytes("f1"), Bytes.toBytes("a"), Bytes.toBytes("aaa-value"), delete);

        //如果存在给定rowKey,family,qualifier,value的记录，那么执行mutate, 使用mutateRow来保证同一个Row下多个put和delete操作的原子性
//        Put put = new Put(Bytes.toBytes("my-row-key0"));
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mu"), Bytes.toBytes("mu-value"));
//
//        Delete delete = new Delete(Bytes.toBytes("my-row-key0"));
//        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("m1"));
//
//        RowMutations rowMutations = new RowMutations(Bytes.toBytes("my-row-key0"));//put/delete的rowKey要跟这里的相同
//        rowMutations.add(put);
//        rowMutations.add(delete);
//        table.mutateRow(rowMutations);
        //table.checkAndMutate(Bytes.toBytes("my-row-key0"), Bytes.toBytes("f1"), Bytes.toBytes("m1"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(""), rowMutations);

        connection.close();
    }
}
