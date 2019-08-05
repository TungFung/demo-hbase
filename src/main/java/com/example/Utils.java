package com.example;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.util.List;

public class Utils {

    public static void printCellsInfo(List<Cell> cells){
        if(CollectionUtils.isEmpty(cells)){
            System.err.println("数据为空");
            return;
        }

        for(Cell cell: cells){
            StringBuffer sb = new StringBuffer();
            sb.append("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
            sb.append("Family:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
            sb.append("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
            sb.append("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
            sb.append("Timestamp:" + cell.getTimestamp());
            System.out.println(sb.toString());
        }
    }
}
