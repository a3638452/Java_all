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
	String SPARK_LOCAL = "local";
	String SPARK_PAGE_RATE = "PageConvertRate";
	String SPARK_SQL_DIR= "spark.sql.warehouse.dir";
	String WAREHOURSE_DIR = "file:${system:user.dir}/spark-warehouse";	
	
	
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
	
	
}
