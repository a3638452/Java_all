package com.orange.common.util;
/*
 * 功能：通过给静态方法get_SessionOfSpark()传递一个参数"appname"和一个"dataTarg"来得到一个sparkSession对象
 * 时间：2016.01.03
 * 作者：大数据部门-任乐乐
 */
import org.apache.spark.sql.SparkSession;

public class SessionOfSpark {
	public static SparkSession getSparkSQLSession(String appName,String dataTarg){
		SparkSession spark = SparkSession
			  .builder()
			  .appName(appName)
			  .config(Constants.SPARK_SQL_DIR, Constants.WAREHOURSE_DIR)
			  .enableHiveSupport()
			  .getOrCreate();
		spark.sql(dataTarg);
		return spark;
	}
}
