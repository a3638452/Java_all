package com.orange.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;

/**
 * 网络类型指标
 * @author Administrator
 *
 */
public class NetworkType {
	
	public NetworkType netWorkType( SparkSession ss){
		 Dataset<Row> rowDS = ss.sql("select devicenetwork network_type ,count(distinct userid)  user_count ,count(userid) login_count,"
		 		+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
		 		+" from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time"
		 		+" from logindata where logintime > from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') group by devicenetwork");
		 //导入mysql
		 rowDS.write().mode("append").jdbc(Constants.JDBC_URL, Constants.JDBC_TABLE_NETWORK, Constants.JdbcCon());
		return null;
		
	}
	
}
