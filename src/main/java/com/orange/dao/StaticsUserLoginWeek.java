package com.orange.dao;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;
import com.orange.common.util.SparkSessionHive;


public class StaticsUserLoginWeek implements Serializable{

	private static final long serialVersionUID = 1L;

	public void staticsUserLoginWeek(){
		
		//1.构建sparksession
		  SparkSession spark = new SparkSessionHive().getSparkSession();
		  
		   spark.sql("use sdkdata");
		    spark.sql("select distinct userid,"
		    		+ "count(distinct substr(logintime,1,10)) day_times "
		    		+ "from logindata "
		    		+ "where logintime >=FROM_UNIXTIME(UNIX_TIMESTAMP()-604800,'yyyy-MM-dd') "
		    		+ "group by userid")
		    .createOrReplaceTempView("t_logindata");
		    
		    spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
		    .createOrReplaceTempView("t_user_base");
		    
		    //需要把这个结构表放到hdfs的一个文件夹下，每天生成一个文件
		   Dataset<Row> userModuleUseTimeDSet = spark.sql("SELECT "
		   			+ "a.userid,"
		    		+ "b.s_user_name user_name,"
		    		+ "b.s_type user_type,"
		    		+ "b.s_xiaoxincode xiaoxin_code,"
		    		+ "a.day_times login_days ,"
		    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-604800,'yyyy-MM-dd') report_start_date, "
		    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') report_end_date "
		    		+ " FROM t_logindata a, t_user_base b "
		    		+ " WHERE  a.userid = b.p_id ");
		   userModuleUseTimeDSet.write().mode("append").jdbc(Constants.JDBC_TEST_EXIAOXIN, Constants.T_USER_LOGIN_WEEK, Constants.testJdbcCon());
		   
		   spark.stop();
		
		
	}
}
