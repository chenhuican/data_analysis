package com.gst.spark.project.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 *  MySQL数据库连接池
 *  @author huican.chen
 */
public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection(){
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://192.168.1.234:3306/DbReport?characterEncoding=utf8",
                            "xletl",
                            "G05st016"
                    );
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }

}
