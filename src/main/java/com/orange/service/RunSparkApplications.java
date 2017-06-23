package com.orange.service;

import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SparkSessionHDFS;


/**
 * 用户指标的run方法
 * @author Administrator
 *
 */
public class RunSparkApplications {

	public static void main(String[] args) {
		SparkSession sparkHDFS = new SparkSessionHDFS().getSparkSession();
		
		 new QueAnswer().getQqueAnswer(sparkHDFS);  //你问我答每天答题量统计 
		 new StatisticsUserInforLoginTime().statisticsUserInforLoginTime(sparkHDFS);   //用户登陆时长
		 new DayUserUseTime().dayUserUseTime(sparkHDFS);  //停留时长人数统计
		 
		 sparkHDFS.stop(); 
	}
}
