package com.orange.common.util;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * Spark作业相关的常量
	 */
	String SPARK_LOCAL = "spark.local";
	String SPARK_PAGE_RATE = "PageConvertRate";
	String SPARK_SQL_DIR= "spark.sql.warehouse.dir";
	String WAREHOURSE_DIR = "file:${system:user.dir}/spark-warehouse";	
	String WAREOURSELOCATION = "/code/VersionTest/spark-warehouse";
	
	String HDFS_URL_ANDROID_1D = "hdfs://master:9000/SDKData/android/data_android_" + DateUtils.getYesterdayDate2() + "/pagedata_android" + DateUtils.getYesterdayDate2() + ".txt";
	String HDFS_URL_ANDROID_7D = "hdfs://master:9000/user/hadoop/bingbing/android/pagedata7day.txt";
	String HDFS_URL_ANDROID_14D = "hdfs://master:9000/user/hadoop/bingbing/android/pagedata14day.txt";
	String HDFS_URL_ANDROID_30D = "hdfs://master:9000/user/hadoop/bingbing/android/pagedata30day.txt";
	String HDFS_URL_IOS_1D = "hdfs://master:9000/SDKData/ios/data_ios_" + DateUtils.getYesterdayDate2() + "/pagedata_ios" + DateUtils.getYesterdayDate2() +".txt";
	String HDFS_URL_IOS_7D = "hdfs://master:9000/user/hadoop/bingbing/ios/pagedata7day.txt";
	String HDFS_URL_IOS_14D = "hdfs://master:9000/user/hadoop/bingbing/ios/pagedata14day.txt";
	String HDFS_URL_IOS_30D = "hdfs://master:9000/user/hadoop/bingbing/ios/pagedata30day.txt";
	
	
	/**
	 * 项目配置相关的常量
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	
	String JDBC_URL2 = "url";
	String JDBC_USER2 = "user";
	String JDBC_PASSWORD2 = "password";
	
	
	
}
