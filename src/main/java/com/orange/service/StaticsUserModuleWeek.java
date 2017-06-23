package com.orange.service;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;


public class StaticsUserModuleWeek implements Serializable{

		private static final long serialVersionUID = 1L;

		public  void staticsUserModuleWeek(SparkSession spark) {
			
			   spark.sql("use sdkdata");
			    spark.sql("SELECT userid,"
			    		+ "pagename,"
			    		+ "unix_timestamp(logouttime)-unix_timestamp(logintime) as use_time"
			    		+ " FROM pagedata"
			    		+ " WHERE logintime is not null and logintime >=FROM_UNIXTIME(UNIX_TIMESTAMP()-604800,'yyyy-MM-dd') ")
			    .createOrReplaceTempView("t_page_use_time");
			    
			    spark.sql("select a.userid ,"
			    		+ "b.modulename module,"
			    		+ "count(a.userid) pv,"
			    		+ "sum(a.use_time) use_time"
			    		+ " FROM t_page_use_time a , page_module_map b "
			    		+ " WHERE a.pagename=b.pagename AND a.use_time >='0' "
			    		+ " GROUP by a.userid,b.modulename")
			    		.createOrReplaceTempView("t_user_modules_time");
			    
			    spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
			    .createOrReplaceTempView("t_user_base");
			    
			   Dataset<Row> userModuleUseTimeDSet = spark.sql("SELECT "
			   			+ "a.userid,"
			    		+ "b.s_user_name user_name,"
			    		+ "b.s_type user_type,"
			    		+ "b.s_xiaoxincode xiaoxin_code,"
			    		+ "b.s_phone1,"
			    		+ "a.module,"
			    		+ "a.pv,"
			    		+ "a.use_time ,"
			    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-604800,'yyyy-MM-dd') report_start_date, "
			    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') report_end_date "
			    		+ " FROM t_user_modules_time a, t_user_base b "
			    		+ " WHERE  a.userid = b.p_id AND  a.module is not null");
			   userModuleUseTimeDSet.write().mode("append").jdbc(Constants.JDBC_TEST_EXIAOXIN, Constants.T_USER_MODULE_WEEK, Constants.testJdbcCon());
			   
			   
		
		}
}
