package com.orange.service;


import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SessionOfSpark;
import com.orange.dao.AprilCount;

public class AprilCountRun {
	public static void main(String[] args) throws Exception {
		
		//conf.properties配置为本地环境
		
//		SparkSession session = SessionOfSpark.getSparkSQLSession("AprilReportSystem");
//		new AprilCount().classUser(session);//report_class_user
//		new AprilCount().schoolCard(session);//report_school_card
//		new AprilCount().SelledProduct(session);//report_selled_count
//		new AprilCount().schoolType(session);//report_school_type
//		new AprilCount().classcount(session);//report_class_count
//		new AprilCount().SelledNear(session);//report_selled_near		
//		new AprilCount().schoolService(session);//2017六月修改report_selled_school_count report_selled_school_count_test 加入了t_use_school_count列  
//		session.stop();
		
		SparkSession sessionmongo = SessionOfSpark.getSparkSQLSession("AprilReportSystem","use mongodata");
		new AprilCount().apiMonitor(sessionmongo);//2017-06-13 完成，api 积分相关接口的使用监控
		sessionmongo.stop();
		
		SparkSession sessionsdk = SessionOfSpark.getSparkSQLSession("AprilReportSystem","use sdkdata");
		new AprilCount().pageCount(sessionsdk);//2017六月增加 ：统计页面点击量 ok
		new AprilCount().openCount(sessionsdk);//2017六月增加 ：统计app的启动次数 ok
		new AprilCount().trackCount(sessionsdk);//2017-06-13 完成，统计用户轨迹
		new AprilCount().daysTrackCount(sessionsdk);//2017-06-13 完成，统计系统轨迹
		sessionsdk.stop();
		
		
		
//		new AprilCount().userTypeDau(session);//已经放入日常指标中
		
		
		
		
	}
}
