package com.orange.service;

import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;
import com.orange.dao.ModuleUserCount;
import com.orange.dao.NetworkType;
import com.orange.dao.SysAppversion;
import com.orange.dao.SysOsVersion;

/**
 * 主程序方法入口
 * @author Administrator
 *
 */
public class ReportRun {
	public static void main(String[] args) {
		SparkSession ss = SparkSession
				 .builder()
			     .appName("ReportSys")
			     .config(Constants.SPARK_SQL_DIR, Constants.WAREHOURSE_DIR)
			     .enableHiveSupport()
			     .getOrCreate();
				 ss.sql("use sdkdata");
	//网络类型指标	
	new NetworkType().netWorkType(ss);
	//app版本
	new SysAppversion().sysAppVersion(ss);
	//手机系统版本
	new SysOsVersion().sysOsVersion(ss);
	//模块使用量
    new ModuleUserCount().moduleUserCount(ss);
    
    //执行完毕，退出
	ss.stop();
	
 }
}