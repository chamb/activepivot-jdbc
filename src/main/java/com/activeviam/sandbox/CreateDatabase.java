/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.sandbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test class to create and populate a database.
 * 
 * @author ActiveViam
 *
 */
public class CreateDatabase {

	// JDBC driver name and database URL 
	static final String JDBC_DRIVER = "org.h2.Driver";   
	static final String DB_URL = "jdbc:h2:mem:test;INIT=RUNSCRIPT FROM 'classpath:create.sql'";
	   
	//  Database credentials 
	static final String USER = "sa"; 
	static final String PASS = "";
	
	public static void main(String[] args) throws Exception {

		//STEP 1: Register JDBC driver 
		Class.forName(JDBC_DRIVER);

	    try(Connection conn = DriverManager.getConnection(DB_URL,USER,PASS);
	    	Statement stmt = conn.createStatement()) {

		    String sql;
		    ResultSet result;
		         
		     sql = "SELECT COUNT(*) AS COUNT FROM PRODUCTS";
		     result = stmt.executeQuery(sql);
		     while(result.next()) {
		    	 System.out.println("productCount=" + result.getLong("COUNT"));
		     }
		         
		     sql = "SELECT COUNT(*) AS COUNT FROM TRADES";
		     result = stmt.executeQuery(sql);
		     while(result.next()) {
		    	 System.out.println("tradeCount=" + result.getLong("COUNT"));
		     }
		         
		     sql = "SELECT COUNT(*) AS COUNT FROM RISKS";
		     result = stmt.executeQuery(sql);
		     while(result.next()) {
		    	 System.out.println("riskCount=" + result.getLong("COUNT"));
		     }
	     } catch(SQLException se) { 
	        //Handle errors for JDBC 
	        se.printStackTrace(); 
	     }

	}

}
