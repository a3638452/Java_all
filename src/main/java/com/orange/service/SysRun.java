package com.orange.service;
/*
 * 功能：通过实例化功能类，产生功能类对象，调用功能方法实现系统指标的产生和导出
 * 时间：2016.12.29
 * 作者：大数据部门-任乐乐
 */
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SessionOfSpark;
import com.orange.dao.CountSystemIndex;


public class SysRun {
	public static void main(String[] args) {	
		
		SparkSession session = SessionOfSpark.getSparkSQLSession("DBReportSystem","use sdkdata");
		new CountSystemIndex().get_sys_screentype(session);//统计系统指标：用户在不同设备分辨率中的分布,数据源为sdkdata
		new CountSystemIndex().get_sys_networktype(session);//统计系统指标：用户在不同网络类型中的分布,数据源为sdkdata

		session.sql("use exiaoxin");
		new CountSystemIndex().get_sys_appversion(session);//统计系统指标：用户在不同版本“e学”中的分布,数据源为exiaoxin
		new CountSystemIndex().get_sys_devicetype(session);//统计系统指标：用户在不同设备型号中的分布,数据源为exiaoxin
		new CountSystemIndex().get_sys_osversion(session);//统计系统指标：用户在不同操作系统版本中的分布,数据源为exiaoxin
		session.stop();	
		
	}
}