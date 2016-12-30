package com.orange.service;
/*
 * 功能：通过实例化功能类，产生功能类对象，调用功能方法实现指标的产生和导出
 * 时间：2016.12.09
 * 作者：大数据部门-任乐乐
 */
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SessionOfSpark;
import com.orange.dao.CountUserIndex;


public class Run {
	public static void main(String[] args) {
		SparkSession session = SessionOfSpark.getSparkSQLSession("DBReportSystem","use sdkdata");
		new CountUserIndex().get_count_devicetype(session);//获取设备机型分布
		/*new CountUserIndex().get_count_devicescreen(session);//获取设备分辨率
		new CountUserIndex().get_count_area(session);//获取用户的分布区域
		new CountUserIndex().get_dau(session);//统计app的日活跃用户数
		new CountUserIndex().get_sevendau(session);//统计app的7天/周活跃用户数
		new CountUserIndex().get_thirtydau(session);//统计app的30天/月活跃用户数
		new CountUserIndex().get_hau(session);//统计app的分时用户活跃情况
		new CountUserIndex().get_count_modules_hau(session);//统计模块日活跃情况
		new CountUserIndex().get_count_stay_time(session);//统计用户停留时长
		new CountUserIndex().get_count_stay_time(session);//统计用户使用频率
		new CountUserIndex().get_count_AdClickTimeSection(session);//统计广告日点击情况
*/		session.stop();
	}
}