package com.orange.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;
import com.orange.common.util.SparkSessionHDFS;

/**
 * 每天用户提问、回答、回答被采纳数量统计
 * @author Administrator
 *
 */
public class UserQueAcceptCount {

	public static void main(String[] args) {

		//1.构建sparksession
		  SparkSession spark = new SparkSessionHDFS().getSparkSession();
		
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_PLAT_QUE, Constants.JdbcCon())
		  .createOrReplaceTempView("t_plat_que");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
		  .createOrReplaceTempView("t_plat_que");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_PLAT_ANSWER, Constants.JdbcCon())
		  .createOrReplaceTempView("t_plat_que");
		 //用户提问数
		Dataset<Row> sql = spark.sql("SELECT s_xiaoxincode,"
				+ "s_user_name ,"
				+ "COUNT(s_creator) s_que_count,"
				+ "SUBSTRING(s_create_time,1,10) s_report_date "+
				"FROM(SELECT  "+
				"a.s_creator, "+
				"b.s_xiaoxincode, "+
				"b.s_user_name, "+
				"a.s_content, "+
				"a.s_create_time "+
				 "FROM t_plat_que a,t_user_base b "+
				 "WHERE a.s_creator=b.p_id AND a.s_create_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND CURRENT_DATE()) t "+
				 "GROUP BY s_xiaoxincode,s_user_name");
		sql.write().mode("append").jdbc(Constants.JDBC_TEST_EXIAOXIN, Constants.T_USER_QUE_NUM, Constants.testJdbcCon());
		
		 //回答数、采纳数
		Dataset<Row> sql_02 = spark.sql("");
		sql_02.write().mode("append").jdbc(Constants.JDBC_TEST_EXIAOXIN, Constants.T_ANS_ACCEPT, Constants.testJdbcCon());
		
	}

}
