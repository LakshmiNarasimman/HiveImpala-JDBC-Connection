package com.jdbc.hive;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException, IOException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://10.130.121.181:10000/poc", "hduser", "");
		Statement stmt = con.createStatement();
		String tableName = "patient";

		String sql = "select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr='70836900000000' and sdate between '2017-06-06' and '2017-06-06' limit 1000000";
		System.out.println("Running: " + sql);
		long sTimeE=System.currentTimeMillis();
		ResultSet res = stmt.executeQuery(sql);
		System.out.println("execution time: "+(System.currentTimeMillis()-sTimeE));
		 FileWriter writer = new FileWriter("/home/bigdata/bigdata_noahdata/JDBC-HIVE-FILE/sms_log_ts.csv");  
		    BufferedWriter buffer = new BufferedWriter(writer);  
		    long sTimeFile=System.currentTimeMillis();
		    int columnCount = res.getMetaData().getColumnCount();
		    StringBuffer s = new StringBuffer();
		    while (res.next()) {
		    	
		    	for (int i = 1; i <= columnCount; i++) {
					s.append(res.getObject(i)+",");
		    	}
		    buffer.write(s.toString()+"\n");  
		}
		    System.out.println("file write time: "+(System.currentTimeMillis()-sTimeFile));
		    buffer.close();  
		    System.out.println("Success");  

	}
}