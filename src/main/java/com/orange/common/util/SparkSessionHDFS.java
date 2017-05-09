package com.orange.common.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionHDFS {

	/**
	 * 配置spark on HDFS作业环境
	 * @return
	 */
	
	public  SparkSession getSparkSession() {
		
	    SparkSession spark = SparkSession
	      .builder()
	      //.master(Constants.SPARK_LOCAL)
	      .appName(Constants.SPARK_REPORT)
	      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
	      .getOrCreate();

		return spark;
	}
}

