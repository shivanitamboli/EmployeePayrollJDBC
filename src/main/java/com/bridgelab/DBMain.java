package com.bridgelab;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
public class DBMain {
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
        String userName = "shivani";
        String password = "1234";
        Connection connection;

        //checking the driver is loaded or not
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Driver loaded!");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot find the driver in the classpath!", e);
        }
        listDrivers();

        // establishing connection
        try {
            System.out.println("Connecting to database : " + jdbcURL);
            connection = DriverManager.getConnection(jdbcURL, userName, password);
            System.out.println("Connection is successful!!!!" + connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Purpose : To check all the drivers registered with jdbc driver
     */
    private static void listDrivers() {
        Enumeration<Driver> driverList = DriverManager.getDrivers();
        while (driverList.hasMoreElements()) {
            Driver driverClass = driverList.nextElement();
            System.out.println("  " + driverClass.getClass().getName());
        }
    }
}