package com.orange.dao;

/*
 * 功能：统计用户指标：用户点击广告的时间、用户的地理区域分布、用户的日活跃、分辨率分布、设备类型分布、七天（周）活跃、用户停留时长、三十天（月）活跃
 * 时间：2016.12.29
 * 作者：大数据部门-任乐乐
 */

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.dbhelper.ConfigurationManager;
import com.orange.common.dbhelper.PropertiesUtil;
import com.orange.common.util.Constants;

public class CountUserIndex {
	
	//统计广告的分时点击情况，并将结果写入report_ad_click_time表
	public void get_count_AdClickTimeSection (SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "\"首页广告位\" as ad_name,"
			+ "substring_index(substring_index(logintime,':',2),' ',-1) as click_time_section,"
			+ "count(userid) as click_number,"
			+ "count(distinct userid) as user_number,"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from pagedata "
			+ "where functionname like '%首页广告%' "
			+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by substring_index(logintime,':',2)");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_ad_click_time",PropertiesUtil.getProperties());
		Dataset<Row> sqlDF2 = session.sql("select "
			+ "\"线上学堂广告位\" as ad_name,"
			+ "substring_index(substring_index(logintime,':',2),' ',-1) as click_time_section,"
			+ "count(userid) as click_number,"
			+ "count(distinct userid) as user_number,"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from pagedata "
			+ "where functionname like '%线上学堂广告%' "
			+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by substring_index(logintime,':',2)"
			);
		sqlDF2.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_ad_click_time",PropertiesUtil.getProperties());
	}
	
	//统计用户的地理区域分布，并将结果写入sys_user_area表
	public void get_count_area (SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select province,"
				+ "city,"
				+ "area,"
				+ "count(distinct userid) as user_number,"
				+ "count(userid) as login_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where province <> '' "
				+ "and city <> '' "
				+ "and area <> '' "
				+ "and province <> 'null' "
				+ "and city <> 'null' "
				+ "and area <> 'null' "
				+ "and province is not null "
				+ "and city is not null "
				+ "and area is not null "				
				+ "group by province,city,area");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "sys_user_area",PropertiesUtil.getProperties());
	}
	
	//统计用户的日活，并将结果写入report_au和report_daysau表
	public void get_dau (SparkSession session){	
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "province,"
			+ "city,"
			+ "area,"
			+ "count(distinct userid) as active_user_number,"
			+ "count(userid) as login_number,"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') active_date,"
			+ "\"DAU\" as count_type,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where "
			+ "province <> '' "
			+ "and city <> '' "
			+ "and area <> '' "
			+ "and province <> 'null' "
			+ "and city <> 'null' "
			+ "and area <> 'null' "
			+ "and province is not null "
			+ "and city is not null "
			+ "and area is not null "		
			+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by province,city,area");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_au",PropertiesUtil.getProperties());
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_daysau",PropertiesUtil.getProperties());
	}
	
	//获取用户机型分辨率分布，并写入表sys_screen
	public void get_count_devicescreen(SparkSession session) {
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select devicescreen as screen,"
			+ "count(distinct userid) as user_number,"
			+ "count(userid) as login_number,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where devicescreen <> '' "
			+ "group by devicescreen");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "sys_screen",PropertiesUtil.getProperties());
	}

	//获取设备机型分布，并写入表sys_devicetype
	public void get_count_devicetype(SparkSession session) {	
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
				+ "substring_index(devicetype,' ',1) os_type,"
				+ "substring_index(substring_index(devicetype,' ',4),' ',-2) as device_type,"
				+ "count(distinct userid) as user_number,"
				+ "count(userid) as login_number,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where devicetype <> '' "
				+ "and devicetype is not null "	
				+ "group by substring_index(devicetype,' ',1),substring_index(substring_index(devicetype,' ',4),' ',-2) ");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "sys_devicetype",PropertiesUtil.getProperties());
	}

	//获取用户停留时长，并写入表report_hau
	public void get_hau (SparkSession session){	
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "substring_index(substring_index(logintime,':',1),' ',-1) as time_section,"
			+ "count(distinct userid) as active_user_number,"
			+ "count(userid) as login_number,"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') as report_date "
			+ "from logindata "
			+ "where logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by substring_index(substring_index(logintime,':',1),' ',-1) ");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_hau",PropertiesUtil.getProperties());
	}

	//获取模块分时活跃情况，并写入表sys_modules_hau
	public void get_count_modules_hau (SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "modulename as module_name,"
			+ "substring_index(substring_index(logintime,':',1),' ',-1) as time_section,"
			+ "count(distinct userid) as user_number,"
			+ "count(userid) as use_number,"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from pagedata "
			+ "LEFT JOIN page_module_map "
			+ "ON pagedata.pagename=page_module_map.pagename "
			+ "where logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by substring_index(substring_index(logintime,':',1),' ',-1),modulename");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "sys_modules_hau",PropertiesUtil.getProperties());
	}

	//获取三十天活跃，并写入表report_daysau
	public void get_sevendau (SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "province,"
			+ "city,"
			+ "area,"
			+ "count(distinct userid) as active_user_number,"
			+ "count(userid) as login_number,"
			+ "concat(from_unixtime(unix_timestamp()-86400*7,'yyyy-MM-dd~'),"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd')) active_date,"
			+ "\"SevenDAU\" as count_type,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where province <> '' "
			+ "and city <> '' "
			+ "and area <> '' "
			+ "and province <> 'null' "
			+ "and city <> 'null' "
			+ "and area <> 'null' "
			+ "and province is not null "
			+ "and city is not null "
			+ "and area is not null "		
			+ "and logintime>from_unixtime(unix_timestamp()-86400*7,'yyyy-MM-dd 00:00:00') "
			+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by province,city,area");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_daysau",PropertiesUtil.getProperties());
	
		//获取周活跃，并写入表report_au
		Calendar ca = Calendar.getInstance();
		ca.setTime(new Date());
		int week = ca.get(Calendar.DAY_OF_WEEK);
		if(week == 2){
			sqlDF = session.sql("select "
				+ "province,"
				+ "city,"
				+ "area,"
				+ "count(distinct userid) as active_user_number,"
				+ "count(userid) as login_number,"
				+ "concat(from_unixtime(unix_timestamp()-86400*7,'yyyy-MM-dd~'),"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd')) active_date,"
				+ "\"WAU\" as count_type,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where province <> '' "
				+ "and city <> '' "
				+ "and area <> '' "
				+ "and province <> 'null' "
				+ "and city <> 'null' "
				+ "and area <> 'null' "
				+ "and province is not null "
				+ "and city is not null "
				+ "and area is not null "	
				+ "and logintime>from_unixtime(unix_timestamp()-86400*7,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by province,city,area");
			sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_au",PropertiesUtil.getProperties());
		}
	}

	//获取用户停留时长，并写入表report_user_stay_time
	public void get_count_stay_time(SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select " 
			+"staytime as stay_time,"
			+"count(userid) as user_number,"
			+"from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') as report_date,"
			+"from_unixtime(unix_timestamp()) as create_time "
			+"from "
			+"(select "
			+"userid,"
			+"sum(usetime) as staytime "
			+"from systimedata "
			+"where "
			+"logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
			+"and "
			+"logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+"group by userid) "
			+"group by staytime");
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_user_stay_time",PropertiesUtil.getProperties());
	}
	
	//获取用户使用频率，并写入表report_use_frequency
	public void get_use_frequency (SparkSession session){
				Dataset<Row> sqlDF = session.sql("select " 
						+"usefrequency as use_frequency,"
						+"count(userid) as user_number,"
						+"from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') as report_date,"
						+"from_unixtime(unix_timestamp()) as create_time "
						+"from "
						+"(select "
						+"userid,"
						+"count(*) as usefrequency "
						+"from logindata "
						+"where "
						+"logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
						+"and "
						+"logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
						+"group by userid) "
						+"group by usefrequency");
				sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_use_frequency",PropertiesUtil.getProperties());
	} 
	

	//获取用户30天活跃，并写入表report_daysau
	public void get_thirtydau (SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		Dataset<Row> sqlDF = session.sql("select "
			+ "province,"
			+ "city,"
			+ "area,"
			+ "count(distinct userid) as active_user_number,"
			+ "count(userid) as login_number,"
			+ "concat(from_unixtime(unix_timestamp()-86400*30,'yyyy-MM-dd~'),"
			+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd')) active_date,"
			+ "\"ThirtyDAU\" as count_type,"
			+ "from_unixtime(unix_timestamp()) create_time "
			+ "from logindata "
			+ "where province <> '' "
			+ "and city <> '' "
			+ "and area <> '' "
			+ "and province <> 'null' "
			+ "and city <> 'null' "
			+ "and area <> 'null' "
			+ "and province is not null "
			+ "and city is not null "
			+ "and area is not null "		
			+ "and logintime > from_unixtime(unix_timestamp()-86400*30,'yyyy-MM-dd 00:00:00') "
			+ "and logintime < from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
			+ "group by province,city,area");

		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_daysau",PropertiesUtil.getProperties());

		//统计月活，并将结果写入report_au表
		SimpleDateFormat getdate = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat getmonth = new SimpleDateFormat("yyyy-MM");
		Calendar ca = Calendar.getInstance();//得到一个Calendar的实例 
		ca.setTime(new Date()); //设置时间为当前时间 
		int day = ca.get(Calendar.DAY_OF_MONTH);
		if(day==1){
			ca.add(Calendar.MONTH, -1); //月份减1
			Date beforemonth = ca.getTime();
			String beforedate = getdate.format(beforemonth);
			String beforemonthdate = getmonth.format(beforemonth);		
			sqlDF = session.sql("select "
				+ "province,"
				+ "city,"
				+ "area,"
				+ "count(distinct userid) as active_user_number,"
				+ "count(userid) as login_number,'"
				+ beforemonthdate + "' active_date,"
				+ "\"MAU\" as count_type,"
				+ "from_unixtime(unix_timestamp()) create_time "
				+ "from logindata "
				+ "where province <> '' "
				+ "and city <> '' "
				+ "and area <> '' "
				+ "and province <> 'null' "
				+ "and city <> 'null' "
				+ "and area <> 'null' "
				+ "and province is not null "
				+ "and city is not null "
				+ "and area is not null "
				+ "logintime > '"
				+ beforedate + " 00:00:00' "
				+ "and logintime < from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "group by province,city,area");
			sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "report_au",PropertiesUtil.getProperties());
		}
	}
	
}
