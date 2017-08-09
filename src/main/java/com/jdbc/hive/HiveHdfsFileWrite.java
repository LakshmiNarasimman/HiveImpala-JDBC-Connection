package com.jdbc.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveHdfsFileWrite {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws IOException, SQLException {

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}

		Connection con = DriverManager.getConnection(
				"jdbc:hive2://10.130.121.181:10000/poc", "hduser", "");
		System.out.println("connected");

		Statement stmt = con.createStatement();
		String sql1 = "insert overwrite directory '/hive_benchmark/1' select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";// insert overwrite directory '/hive_benchmark/2' select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";
/*		String sql2 = "insert overwrite directory '/hive_benchmark/2' select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";
		String sql3 = "insert overwrite directory '/hive_benchmark/3' select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";
		String sql4 = "insert overwrite directory '/hive_benchmark/4' select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000 and sdate between '2017-06-06' and '2017-07-26' limit 1000000";
		
		stmt.addBatch(sql1);
		stmt.addBatch(sql2);
		stmt.addBatch(sql3);
		stmt.addBatch(sql4);
*/		
		long sTime=System.currentTimeMillis();
		stmt.execute(sql1);
		System.out.println(System.currentTimeMillis()-sTime);
	}
}
