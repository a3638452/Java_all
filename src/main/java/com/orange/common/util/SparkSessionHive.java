package com.orange.common.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionHive {

	/**
	 * 配置spark on hive作业环境
	 * @return
	 */
	
	public  SparkSession getSparkSession() {
		
	    SparkSession spark = SparkSession
	      .builder()
	      .appName(Constants.SPARK_REPORT)
	      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
	      .enableHiveSupport()
	      .getOrCreate();
		return spark;
	}
}

