package com.orange.service;

import org.apache.spark.sql.SparkSession;

import com.orange.common.util.SparkSessionHDFS;

/**
 * 跳出率的主方法入口
 * @author Administrator
 *
 */
public class JumRateRun {

	public static void main(String[] args) {
		
		SparkSession sparkHDFS = new SparkSessionHDFS().getSparkSession();
		
		 new PageJumpRate().pageJumpRate(sparkHDFS);  //页面跳出率
		 new HomePageJumpRate().homePageJumpRate(sparkHDFS);  //e学产品跳出率
		 
		 sparkHDFS.stop(); 
	}

}
