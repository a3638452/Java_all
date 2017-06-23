package com.orange.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.common.util.Constants;


/**
 * e学app模块使用统计指标
 * @author Administrator
 *
 */
public class ModuleUserCount {

	public ModuleUserCount moduleUserCount(SparkSession ss) {
		Dataset<Row> rowDS = ss
				.sql("SELECT from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') count_date"
						+ ",pagename "
						+ ",count(distinct userid) uv"
						+ ",count(userid) pv"
						+ " from pagedata where logintime > from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00')"
						+ " group by pagename");
		rowDS.createOrReplaceTempView("Module_Statitics");

		Dataset<Row> dataset = ss
				.sql("SELECT modulename module,"
						+ "SUM(a.uv) user_count," 
						+ "SUM(a.pv) use_count,"
						+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
						+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time"
						+ " FROM Module_Statitics a"
						+ " LEFT JOIN page_module_map b"
						+ " ON a.pagename=b.pagename GROUP BY b.modulename");

		// 导入mysql
		dataset.write()
				.mode("append")
				.jdbc(Constants.JDBC_URL, Constants.JDBC_TABLE_MODULE_COUNT,
						Constants.JdbcCon());
		return null;

	}

}
