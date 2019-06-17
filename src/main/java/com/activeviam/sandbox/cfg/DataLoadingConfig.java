/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.sandbox.cfg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import com.qfs.msg.IMessageChannel;
import com.qfs.msg.impl.QfsResultSetRow;
import com.qfs.msg.jdbc.IJDBCSource;
import com.qfs.msg.jdbc.impl.JDBCTopic;
import com.qfs.msg.jdbc.impl.NativeJDBCSource;
import com.qfs.source.impl.JDBCMessageChannelFactory;
import com.qfs.store.IDatastore;
import com.qfs.store.impl.SchemaPrinter;
import com.qfs.store.transaction.ITransactionManager;
import com.qfs.util.timing.impl.StopWatch;

/**
 * Spring configuration for data sources
 * 
 * @author ActiveViam
 *
 */
public class DataLoadingConfig {

    private static final Logger LOGGER = Logger.getLogger(DataLoadingConfig.class.getSimpleName());

    @Autowired
    protected Environment env;

    @Autowired
    protected IDatastore datastore;


    
    
	/*
	 * Create an in-memory database and load sample data.
	 * When using an actual external database this step can be removed.
	 */
    @Bean
    @DependsOn(value = "startManager")
    public Void createDatabase() throws Exception {
		
		// Create database from initialisation script, leave the database open
	    try(Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'classpath:create.sql'", "sa", "");
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

    	return null;
    }
    
    
	/*
	 * **************************** Data loading *********************************
	 */
    
    /**
     * @return JDBC Source
     */
    @Bean
    public IJDBCSource<QfsResultSetRow> jdbcSource() {
		/* Initialize the database */
		Properties properties = new Properties();
		properties.setProperty("username", "sa");
		properties.setProperty("password", "");
		
		// Create the JDBC Source
		NativeJDBCSource jdbcSource = new NativeJDBCSource(
				"jdbc:h2:mem:test", // URL
				"org.h2.Driver", // DRIVER
				properties,
				"Sandbox JDBC Source",
				2, // pool size
				5000  // append batch size
		);
		
		// Register topics
		jdbcSource.addTopic(new JDBCTopic("Products", "SELECT * from Products"));
		jdbcSource.addTopic(new JDBCTopic("Trades", "SELECT * from Trades"));
		jdbcSource.addTopic(new JDBCTopic("Risks", "SELECT * from Risks"));
		
		return jdbcSource;
    }
    
    @Bean
    @DependsOn(value = "createDatabase")
    public Void loadData(IJDBCSource<QfsResultSetRow> jdbcSource) throws Exception {
		
    	final ITransactionManager tm = datastore.getTransactionManager();
    	
    	// Load data into ActivePivot
    	final long before = System.nanoTime();
    	
    	// Transaction for TV data
	    tm.startTransaction();
		
		JDBCMessageChannelFactory jdbcChannelFactory = new JDBCMessageChannelFactory(jdbcSource, datastore);
		IMessageChannel<String, QfsResultSetRow> productChannel = jdbcChannelFactory.createChannel("Products");
		IMessageChannel<String, QfsResultSetRow> tradeChannel = jdbcChannelFactory.createChannel("Trades");
		IMessageChannel<String, QfsResultSetRow> riskChannel = jdbcChannelFactory.createChannel("Risks");
		
		jdbcSource.fetch(Arrays.asList(productChannel, tradeChannel, riskChannel));
		
		tm.commitTransaction();
		
    	final long elapsed = System.nanoTime() - before;
    	LOGGER.info("Data load completed in " + elapsed / 1000000L + "ms");
    	
    	printStoreSizes();
    	
    	return null;
    }


	private void printStoreSizes() {

		// Print stop watch profiling
		StopWatch.get().printTimings();
		StopWatch.get().printTimingLegend();

		// print sizes
		SchemaPrinter.printStoresSizes(datastore.getHead().getSchema());
	}

}
