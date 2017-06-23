package com.orange.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;

/**
 * 
 * @author Administrator
 *
 */
public class SysOsVersion {
	
	public SysOsVersion sysOsVersion(SparkSession ss){
		
		Dataset<Row> rowDS = ss.sql("SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(devicetype,' ',1),' ',1) AS os_type,"+
					"SUBSTRING_INDEX(SUBSTRING_INDEX(devicetype,' ',2),' ',-1) AS os_version,"+
					"COUNT(DISTINCT userid) user_count,"+
					"COUNT(userid) login_count ,"+
					"from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd ') report_date,"+
					"from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time"+
					" from logindata"+
					" where logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00')"+
					" GROUP BY (SUBSTRING_INDEX(SUBSTRING_INDEX(deviceType,' ',2),' ',-1)),SUBSTRING_INDEX(SUBSTRING_INDEX(deviceType,' ',1),' ',1)");
		//导入mysql/test
		rowDS.write().mode("append").jdbc(Constants.JDBC_URL, Constants.JDBC_TABLE_SYSOSVERSION, Constants.JdbcCon());
		
		return null;
		
	}
	
	
}
















