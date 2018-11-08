package com.gst.spark.project.utils;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private  HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop.node1:2181,hadoop.node2:2181,hadoop.node3:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop.node1:8020/hbase");

        try{
            admin = new HBaseAdmin(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if(null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        }catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey, String cf, String column, String value ) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        String tableName = "course_clickcount";
        String rowkey = "20171111_88";
        String cf = "info" ;
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }
}
