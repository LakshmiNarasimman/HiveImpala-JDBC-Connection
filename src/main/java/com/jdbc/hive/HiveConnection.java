package com.jdbc.hive;



import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class HiveConnection {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
public ResultSet phpConnection() throws IOException,SQLException
{

	try {
        // Register driver and create driver instance
        Class.forName(driverName);
} catch (ClassNotFoundException ex) {
      ex.printStackTrace();
}

// get connection
System.out.println("before trying to connect");
Connection con = DriverManager.getConnection("jdbc:hive2://10.130.121.181:10000/poc", "hduser", "");
System.out.println("connected");

// create statement
Statement stmt = con.createStatement();

// execute statement
//stmt.executeQuery("show tables");
//String sql="select sdate,esmeaddr,cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000";
String sql="select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr='70836900000000' and sdate between '2017-06-06' and '2017-06-06' limit 1000000";
stmt.execute("set hive.execution.engine=mr");
SimpleDateFormat sdf11 = new SimpleDateFormat("HH:mm:ss");
//stmt.execute("set hive.execution.engine=tez");
Date rs1=new Date();
System.out.println( "ResultSet Start Time:"+sdf11.format(rs1));
ResultSet res = stmt.executeQuery(sql);
Date rs2=new Date();
System.out.println( "ResultSet End Time:"+sdf11.format(rs2));


/*while(res.next())
{
	
	 System.out.println(res.getString(1) + "\t" + res.getLong(2)+ "\t" + res.getString(3));
	 
}*/
return res;
}

public void show(String name)
{
	System.out.println(name);
}
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	
	public static void main(String[] args) throws IOException, SQLException {
		// TODO Auto-generated method stub
		Date d1=new Date();
		SimpleDateFormat sdf22 = new SimpleDateFormat("HH:mm:ss");
      	System.out.println( "Program Start Time:"+sdf22.format(d1));
		HiveConnection obj =new HiveConnection();

	    CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");

		// file name
		final String FILE_NAME = "/home/bigdata/bigdata_noahdata/JDBC-HIVE-FILE/sms_log_ts.csv";
		
		// creating the file object
		File file = new File(FILE_NAME);
		
		// creating file writer object
		FileWriter fw = new FileWriter(file);

		// creating the csv printer object
		CSVPrinter printer = new CSVPrinter(fw, format);

		// reading the query from user as input
		
		
		// printing the result in 'CSV' file
		printer.printRecords(obj.phpConnection());
		
		System.out.println("Query has been executed successfully...");
		
		// closing all resources
		Date d2=new Date();
      	System.out.println("Program End Time:"+ sdf22.format(d2));
		fw.close();

		printer.close();

	   // obj.show();
	}
}

//spark=0:01:51 
//mr=0:02:08  
