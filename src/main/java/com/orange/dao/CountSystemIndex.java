package com.orange.dao;

/*
 * 功能：统计系统指标，用户在不同“e学”版本、设备类型、网络类型、操作系统版本、设备分辨率中的分布
 * 时间：2016.12.29
 * 作者：大数据部门-任乐乐
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.dbhelper.ConfigurationManager;
import com.orange.common.dbhelper.PropertiesUtil;
import com.orange.common.util.Constants;

public class CountSystemIndex {


	//统计系统指标：用户在不同版本“e学”中的分布
	public void get_sys_appversion(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> appVersion = session.sql("select " 
			+ "substring_index(s_device_type,',',1) os_type,"
			+ "substring_index(s_device_type,',',-1) app_version," 
			+ "count(distinct s_user_name) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time " 
			+ "from t_user_base " 
			+ "where s_device_type <> '' "
			+ "and s_device_type <> 'null' " 
			+ "and s_device_type is not null "
			+ "group by substring_index(s_device_type,',',-1),substring_index(s_device_type,',',1)");

		
		appVersion.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "system_app_version",PropertiesUtil.getProperties());
	}
	
	//统计系统指标：用户在不同设备型号中的分布
	public void get_sys_devicetype(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> deviceType = session.sql("select "
			+ "substring_index(substring_index(s_device_type,',',3),',',-1) device_type,"
			+ "count(distinct s_user_name) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from t_user_base "
			+ "where s_device_type <> '' "
			+ "and s_device_type <> 'null' "
			+ "and s_device_type is not null "		
			+ "group by substring_index(substring_index(s_device_type,',',3),',',-1)");
		deviceType.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "system_device_type",PropertiesUtil.getProperties());
	}

	//统计系统指标：用户在不同网络类型中的分布
	public void get_sys_networktype(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> network = session.sql("select "
				+ "devicenetwork as network_type,"
				+ "count(distinct userid) as user_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where devicenetwork <> '' "
				+ "and devicenetwork <> 'null' "
				+ "group by devicenetwork");
		network.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2),"system_network_type",PropertiesUtil.getProperties());
	}

	//统计系统指标：用户在不同操作系统版本中的分布
	public void get_sys_osversion(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> osVersion = session.sql("select "
				+ "substring_index(s_device_type,',',2) os_version,"
				+ "count(distinct s_user_name) as user_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from t_user_base "
				+ "where s_device_type <> '' "
				+ "and s_device_type <> 'null' "
				+ "and s_device_type is not null "				
				+ "group by substring_index(s_device_type,',',2)");
		osVersion.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "system_os_version",PropertiesUtil.getProperties());
	}	
	
	//统计系统指标：用户在不同设备分辨率中的分布
	public void get_sys_screentype(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> screen = session.sql("select "
			+ "devicescreen as screen_type,"
			+ "count(distinct userid) as user_number,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where devicescreen <> '' "
			+ "and devicescreen <> 'null' "
			+ "group by devicescreen");
		screen.write().mode("Overwrite").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2),"system_screen_type",PropertiesUtil.getProperties());		
	}
	

}
