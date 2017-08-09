package com.jdbc.hive;

import java.io.File;


import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class ImpalaConnection {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws SQLException, IOException {
		Date ps1=new Date();
		SimpleDateFormat sdf22 = new SimpleDateFormat("HH:mm:ss");
	  	System.out.println( "Program Start Time:"+sdf22.format(ps1));
		try {
	        // Register driver and create driver instance
	        Class.forName(driverName);
	        System.out.println("Connected to Impala..!");
	} catch (ClassNotFoundException ex) {
	      ex.printStackTrace();
	}

	// get connection
	System.out.println("before trying to connect"); 
	Connection con = DriverManager.getConnection("jdbc:impala://10.130.121.181:21050/poc", "hduser", "");
	System.out.println("connected");

	// create statement
	Statement stmt = con.createStatement();

	// execute statement
	//stmt.executeQuery("show tables");
	//String sql="select sdate,esmeaddr,cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000";
	String sql="select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";
	System.out.println("Query Executed:"+sql);
	Date rs1=new Date();
	System.out.println( "ResultSet Start Time:"+sdf22.format(rs1));
	ResultSet res = stmt.executeQuery(sql);
	Date rs2=new Date();
	System.out.println( "ResultSet End Time:"+sdf22.format(rs2));
	
    CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");

	// file name
	final String FILE_NAME = "/home/bigdata/bigdata_noahdata/Impala/sms_log_4d.csv";
	
	// creating the file object
	File file = new File(FILE_NAME);
	
	// creating file writer object
	FileWriter fw = new FileWriter(file);

	// creating the csv printer object
	CSVPrinter printer = new CSVPrinter(fw, format);

	// reading the query from user as input
	
	
	// printing the result in 'CSV' file
	printer.printRecords(res);
	
	System.out.println("Query has been executed successfully...");
	
	// closing all resources
	Date pe=new Date();
  	System.out.println("End Time:"+sdf22.format(pe));
	fw.close();

	printer.close();

	con.close();
	
	

	}

}
