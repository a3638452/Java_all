package com.orange.common.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionForHdfs {

	/**
	 * 配置spark作业环境
	 * @return
	 */
	public  SparkSession getSparkSession() {
		
	    SparkSession spark = SparkSession
	      .builder()
	      .appName(Constants.SPARK_PAGE_RATE)
	      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
	      .getOrCreate();
	    SparkUtils.Master(spark);
		return spark;
	}
}

