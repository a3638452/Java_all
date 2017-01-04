package com.orange.common.util;

import org.apache.spark.sql.SparkSession;

import com.orange.common.dbhelper.ConfigurationManager;



/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {
	
	/**
	 * 根据当前是否本地测试的配置
	 * 决定，如何设置SparkConf的master
	 */
	public static void Master(SparkSession spark) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local=true) {
			spark.builder().master("local");
		}  
	}
	
	
	
}
