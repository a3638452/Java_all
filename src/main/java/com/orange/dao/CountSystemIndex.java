package com.orange.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.dbhelper.ConfigurationManager;
import com.orange.common.dbhelper.PropertiesUtil;
import com.orange.common.util.Constants;

public class CountSystemIndex {


	public void get_sys_appversion(SparkSession session) {
		Dataset<Row> appVersion = session.sql("select " 
			+ "substring_index(s_device_type,' ',1) os_type,"
			+ "substring_index(s_device_type,',',-1) app_version," 
			+ "count(distinct s_user_name) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time " 
			+ "from t_user_base " 
			+ "where s_device_type <> '' "
			+ "and s_device_type <> 'null' " 
			+ "and s_device_type is not null "
			+ "group by substring_index(s_device_type,',',-1),substring_index(s_device_type,',',1)");

		
		appVersion.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL), "sys_app_version",PropertiesUtil.get_jdbconnect());
	}
	
	public void get_sys_devicetype(SparkSession session) {
		session.sql("use exiaoxin");
		Dataset<Row> deviceType = session.sql("select "
			+ "substring_index(substring_index(s_device_type,',',3),',',-1) device_type,"
			+ "count(distinct s_user_name) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from t_user_base "
			+ "where s_device_type <> '' "
			+ "and s_device_type <> 'null' "
			+ "and s_device_type is not null "		
			+ "group by substring_index(substring_index(s_device_type,',',3),',',-1)");
		deviceType.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL), "sys_device_type",PropertiesUtil.get_jdbconnect());
	}

	public void get_sys_networktype(SparkSession session) {
		session.sql("use sdkdata");
		Dataset<Row> network = session.sql("select "
				+ "devicenetwork as network_type,"
				+ "count(distinct userid) as user_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where devicenetwork <> '' "
				+ "and devicenetwork <> 'null' "
				+ "group by devicenetwork");
		network.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL),"sys_network_type",PropertiesUtil.get_jdbconnect());
	}

	public void get_sys_osversion(SparkSession session) {
		session.sql("use exiaoxin");
		Dataset<Row> osVersion = session.sql("select "
				+ "substring_index(s_device_type,',',2) os_version,"
				+ "count(distinct s_user_name) as user_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from t_user_base "
				+ "where s_device_type <> '' "
				+ "and s_device_type <> 'null' "
				+ "and s_device_type is not null "				
				+ "group by substring_index(s_device_type,',',2)");
		osVersion.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL), "sys_os_version",PropertiesUtil.get_jdbconnect());
	}	
	
	public void get_sys_screentype(SparkSession session) {
		session.sql("use sdkdata");
		Dataset<Row> screen = session.sql("select "
			+ "devicescreen as screen_type,"
			+ "count(distinct userid) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where devicescreen <> '' "
			+ "and devicescreen <> 'null' "
			+ "group by devicescreen");
		screen.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL),"sys_screen_type",PropertiesUtil.get_jdbconnect());		
	}
	

}
