package com.orange.service;

import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SparkSessionHive;


public class RunSparkApplicationsWeek1 {
	/**
	 * 用户指标的run方法
	 * @author Administrator
	 *
	 */
	public static void main(String[] args) {
		  SparkSession spark = new SparkSessionHive().getSparkSession();
		  
		new StaticsUserModuleWeek().staticsUserModuleWeek(spark); //每周一跑的上周用户模块使用量、停留时长	
		new StaticsUserLoginWeek().staticsUserLoginWeek(spark); //上周用户登陆天数
		new StaticsUserActiveFrequency().staticsUserActiveFrequency(spark); //每周一跑的区域用户生命周期指标

		spark.stop();
	}

}
