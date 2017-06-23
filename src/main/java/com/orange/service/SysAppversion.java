
package com.orange.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;

/**
 * app系统版本指标
 * @author Administrator
 *
 */
public class SysAppversion {

	public SysAppversion sysAppVersion(SparkSession ss){
		Dataset<Row> rowDS = ss.sql("SELECT  SUBSTRING_INDEX(SUBSTRING_INDEX(deviceType,' ',1),' ',1) os_type" +
				 ",SUBSTRING_INDEX(deviceType,' ',-1) app_version" +
				 ",COUNT(DISTINCT userid) user_count" +
				 ",COUNT(userid) login_count" +
				 ", from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date" +
				 ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time" +
				 " from logindata" +
				 " where logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00')" +
				 " GROUP BY (SUBSTRING_INDEX(deviceType,' ',-1)),SUBSTRING_INDEX(SUBSTRING_INDEX(deviceType,' ',1),' ',1)");
		 //导入mysql/test
		 rowDS.write().mode("append").jdbc(Constants.JDBC_URL, Constants.JDBC_TABLE_SYS_APPVERSION, Constants.JdbcCon());
		return null;
		
	}
	

}

