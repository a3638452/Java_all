package com.orange.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/*
 * 功能：统计系统指标，用户在不同“e学”版本、设备类型、网络类型、操作系统版本、设备分辨率中的分布
 * 时间：2016.12.29
 * 作者：大数据部门-任乐乐
 * 修改：2017-06-05，修改为更新方式（读取结果表，读取前一天的数据进行统计，结果与之前的历史结果进行求和）
 */

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.dbhelper.ConfigurationManager;
import com.orange.common.dbhelper.PropertiesUtil;
import com.orange.common.util.Constants;

public class CountSystemIndex {


	//统计系统指标：用户在不同版本“e学”中的分布
	public void get_sys_appversion(SparkSession session) throws SQLException {
		
		//1.读取，存入临时表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_all_app_version",PropertiesUtil.getProperties()).createOrReplaceTempView("t_hs");
		
		//2.读入昨日一天的数据，计算，结果存入临时表
		session.sql("select "
				+ "substring_index(devicetype,' ',1) os_type,"
				+ "substring_index(devicetype,' ',-1) app_version,"
				+ "count(distinct userid) as user_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "				
				+ "from logindata "
				+ "where devicetype <> '' "
				+ "and devicetype <> 'null' "
				+ "and logintime>=from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "and devicetype like '%.%.%' "
				+ "and devicetype is not null "		
				+ "group by substring_index(devicetype,' ',1),substring_index(devicetype,' ',-1)").createOrReplaceTempView("t_1d");
		
		//3.两个临时表合并，对应相加
		List<Row>  appVersion = session.sql("select "
				+ "t_hs.os_type os_type,"
				+ "t_hs.app_version app_version,"
				+ "t_hs.user_count+t_1d.user_count user_count,"
				+ "t_1d.report_date report_date,"
				+ "t_1d.create_time create_time "
				+ "from t_hs INNER JOIN t_1d ON t_hs.os_type=t_1d.os_type and t_hs.app_version=t_1d.app_version ").collectAsList();
		
		//4.清空表
		String user = "xxv2";
		String password = "xv2PassWD-321";
		Connection con2 = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);
		
		String sql2 = "DELETE FROM report_all_app_version";		
		PreparedStatement pst0 = con2.prepareStatement(sql2);
		pst0.addBatch();
		pst0.executeBatch(); // insert remaining records
		pst0.close();
		con2.close();
		
		
		String sql = "INSERT INTO report_all_app_version (os_type,app_version,user_count,report_date,create_time) VALUES (?, ?, ?, ?, ?)";
		Connection connection = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);	
		PreparedStatement pst = connection.prepareStatement(sql);
		final int batchSize = 1000;
		int count = 0;
		
		for (Row st : appVersion) {
			String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
			pst.setString(1, re[0]);
			pst.setString(2, re[1]);
			pst.setInt(3, Integer.parseInt(re[2]));
			pst.setString(4, re[3]);
			pst.setString(5, re[4]);
			pst.addBatch();
			if (++count % batchSize == 0) {
				try {
					pst.executeBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		pst.executeBatch(); // insert remaining records
		pst.close();
		connection.close();
		
		
		
	
	}

	//统计系统指标：用户在不同设备型号中的分布
	public void get_sys_devicetype(SparkSession session) throws SQLException {
		
		//1.读取，存入临时表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_all_devices_type",PropertiesUtil.getProperties()).createOrReplaceTempView("t_hs");
				
		//2.读入昨日一天的数据，计算，结果存入临时表
		session.sql("select "
				+ "substring_index(substring_index(devicetype,' ',4),' ',-2) as device_type,"
				+ "count(distinct userid) as user_count,"
				+ "count(userid) as login_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "from logindata "
				+ "where devicetype <> '' "
				+ "and devicetype is not null "	
				+ "and devicetype not like '%省%' "	
				+ "and devicetype not like '%市%' "
				+ "and devicetype not like '%区%' "
				+ "and devicetype not like '%县%' "
				+ "and devicetype not like '%路%' "
				+ "and logintime>=from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by substring_index(devicetype,' ',1),substring_index(substring_index(devicetype,' ',4),' ',-2) ").createOrReplaceTempView("t_1d");
		
		//3.两个临时表合并，对应相加
		List<Row> deviceType = session.sql("select "
				+ "t_hs.device_type device_type,"
				+ "t_hs.user_count+t_1d.user_count user_count,"
				+ "t_hs.login_count+t_1d.login_count login_count,"
				+ "t_1d.report_date,"
				+ "t_1d.create_time "
				+ "from t_hs INNER JOIN t_1d ON t_hs.device_type=t_1d.device_type ").collectAsList();
		
		
		
		//4.清空表
				String user = "xxv2";
				String password = "xv2PassWD-321";
				Connection con2 = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);
				
				String sql2 = "DELETE FROM report_all_devices_type";		
				PreparedStatement pst0 = con2.prepareStatement(sql2);
				pst0.addBatch();
				pst0.executeBatch(); // insert remaining records
				pst0.close();
				con2.close();
				
				
				String sql = "INSERT INTO report_all_devices_type (device_type,user_count,login_count,report_date,create_time) VALUES (?, ?, ?, ?, ?)";
				Connection connection = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);	
				PreparedStatement pst = connection.prepareStatement(sql);
				final int batchSize = 1000;
				int count = 0;
				
				for (Row st : deviceType) {
					String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
					pst.setString(1, re[0]);
					pst.setInt(2, Integer.parseInt(re[1]));
					pst.setInt(3, Integer.parseInt(re[2]));
					pst.setString(4, re[3]);
					pst.setString(5, re[4]);
					pst.addBatch();
					if (++count % batchSize == 0) {
						try {
							pst.executeBatch();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				pst.executeBatch(); // insert remaining records
				pst.close();
				connection.close();
		

	}

	//统计系统指标：用户在不同网络类型中的分布
	public void get_sys_networktype(SparkSession session) throws SQLException {
		

		//1.读取，存入临时表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_all_network_type",PropertiesUtil.getProperties()).createOrReplaceTempView("t_hs");
						
		//2.读入昨日一天的数据，计算，结果存入临时表
		session.sql("select "
				+ "devicenetwork as network_type,"
				+ "count(distinct userid) as user_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "from logindata "
				+ "where devicenetwork <> '' "
				+ "and devicenetwork <> 'null' "
				+ "and logintime>=from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by devicenetwork").createOrReplaceTempView("t_1d");
		
		//3.两个临时表合并，对应相加
		List<Row> network = session.sql("select "
				+ "t_hs.network_type network_type,"
				+ "t_hs.user_count+t_1d.user_count user_count,"
				+ "t_1d.report_date report_date,"
				+ "t_1d.create_time create_time "
				+ "from t_hs INNER JOIN t_1d ON t_hs.network_type=t_1d.network_type ").collectAsList();
		
		
		
		//4.清空表
				String user = "xxv2";
				String password = "xv2PassWD-321";
				Connection con2 = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);
				
				String sql2 = "DELETE FROM report_all_network_type";		
				PreparedStatement pst0 = con2.prepareStatement(sql2);
				pst0.addBatch();
				pst0.executeBatch(); // insert remaining records
				pst0.close();
				con2.close();
			
				String sql = "INSERT INTO report_all_network_type (network_type,user_count,report_date,create_time) VALUES (?, ?, ?, ?)";
				Connection connection = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);	
				PreparedStatement pst = connection.prepareStatement(sql);
				final int batchSize = 1000;
				int count = 0;
				
				for (Row st : network) {
					String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
					pst.setString(1, re[0]);
					pst.setInt(2, Integer.parseInt(re[1]));
					pst.setString(3, re[2]);
					pst.setString(4, re[3]);
					pst.addBatch();
					if (++count % batchSize == 0) {
						try {
							pst.executeBatch();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				pst.executeBatch(); // insert remaining records
				pst.close();
				connection.close();
		
		
		
		
		
		
	}

	//统计系统指标：用户在不同操作系统版本中的分布iOS 10.2.1 iPhone 6s Plus 1.1.8
	public void get_sys_osversion(SparkSession session) throws SQLException {
		
		//1.读取，存入临时表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_all_os_version",PropertiesUtil.getProperties()).createOrReplaceTempView("t_hs");
		
		
		//2.读入昨日一天的数据，计算，结果存入临时表
		session.sql("select "
				+ "substring_index(devicetype,' ',1) os_type,"
				+ "substring_index(substring_index(devicetype,' ',2),' ',-1) os_version,"
				+ "count(distinct userid) as user_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "from logindata "
				+ "where devicetype <> '' "
				+ "and devicetype <> 'null' "
				+ "and devicetype is not null "	
				+ "and devicetype not like '%省%' "
				+ "and devicetype not like '%市%' "
				+ "and devicetype not like '%区%' "
				+ "and devicetype not like '%县%' "
				+ "and devicetype not like '%路%' "
				+ "and logintime>=from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by substring_index(devicetype,' ',1),substring_index(substring_index(devicetype,' ',2),' ',-1) ").createOrReplaceTempView("t_1d");
		
		
		//3.两个临时表合并，对应相加
		List<Row> osVersion = session.sql("select "
				+ "t_hs.os_type os_type,"
				+ "t_hs.os_version os_version,"				
				+ "t_hs.user_count+t_1d.user_count user_count,"
				+ "t_1d.report_date report_date,"
				+ "t_1d.create_time create_time "
				+ "from t_hs INNER JOIN t_1d ON t_hs.os_type=t_1d.os_type and t_hs.os_version=t_1d.os_version ").collectAsList();
		//4.清空表
				String user = "xxv2";
				String password = "xv2PassWD-321";
				Connection con2 = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);
				
				String sql2 = "DELETE FROM report_all_os_version";		
				PreparedStatement pst0 = con2.prepareStatement(sql2);
				pst0.addBatch();
				pst0.executeBatch(); // insert remaining records
				pst0.close();
				con2.close();
			
				String sql = "INSERT INTO report_all_os_version (os_type,os_version,user_count,report_date,create_time) VALUES (?, ?, ?, ?, ?)";
				Connection connection = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);	
				PreparedStatement pst = connection.prepareStatement(sql);
				final int batchSize = 1000;
				int count = 0;
				
				for (Row st : osVersion) {
					String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
					pst.setString(1, re[0]);
					pst.setString(2, re[1]);
					pst.setInt(3, Integer.parseInt(re[2]));
					pst.setString(4, re[3]);
					pst.setString(5, re[4]);
					pst.addBatch();
					if (++count % batchSize == 0) {
						try {
							pst.executeBatch();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				pst.executeBatch(); // insert remaining records
				pst.close();
				connection.close();
		
		
		
		
		
		
		
	}	
	
	//统计系统指标：用户在不同设备分辨率中的分布
	public void get_sys_screentype(SparkSession session) throws SQLException {
		
		
		//1.读取，存入临时表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_all_screens",PropertiesUtil.getProperties()).createOrReplaceTempView("t_hs");
				
		
		//2.读入昨日一天的数据，计算，结果存入临时表
		session.sql("select "
				+ "devicescreen as screen_type,"
				+ "count(distinct userid) as user_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "from logindata "
				+ "where devicescreen <> '' "
				+ "and devicescreen <> 'null' "
				+ "and devicescreen not like '%省%' "
				+ "and devicescreen not like '%市%' "
				+ "and devicescreen not like '%区%' "
				+ "and devicescreen not like '%县%' "
				+ "and devicescreen not like '%路%' "
				+ "and devicescreen like '%*%' "
				+ "and logintime>=from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by devicescreen").createOrReplaceTempView("t_1d");
		
		
		
		//3.两个临时表合并，对应相加
		List<Row> screen = session.sql("select "
				+ "t_hs.screen_type screen_type,"
				+ "t_hs.user_count+t_1d.user_count user_count,"
				+ "t_1d.report_date report_date,"
				+ "t_1d.create_time create_time "
				+ "from t_hs INNER JOIN t_1d ON t_hs.screen_type=t_1d.screen_type ").collectAsList();
		//4.清空表
		String user = "xxv2";
		String password = "xv2PassWD-321";
		Connection con2 = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);
		
		String sql2 = "DELETE FROM report_all_screens";		
		PreparedStatement pst0 = con2.prepareStatement(sql2);
		pst0.addBatch();
		pst0.executeBatch(); // insert remaining records
		pst0.close();
		con2.close();
	
		String sql = "INSERT INTO report_all_screens (screen_type,user_count,report_date,create_time) VALUES (?, ?, ?, ?)";
		Connection connection = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL2), user, password);	
		PreparedStatement pst = connection.prepareStatement(sql);
		final int batchSize = 1000;
		int count = 0;
		
		for (Row st : screen) {
			String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
			pst.setString(1, re[0]);
			pst.setInt(2, Integer.parseInt(re[1]));
			pst.setString(3, re[2]);
			pst.setString(4, re[3]);
			pst.addBatch();
			if (++count % batchSize == 0) {
				try {
					pst.executeBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		pst.executeBatch(); // insert remaining records
		pst.close();
		connection.close();
		
	}
	

}
