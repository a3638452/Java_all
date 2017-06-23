package com.orange.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.common.util.Constants;
import com.orange.common.util.SparkSessionHive;

/**
 * 区域模块使用人数及时长
 * @author Administrator
 */

public class StatisticsAreaModules  {

		public static void main(String[] args) {
			
			SparkSession sparkHive = new SparkSessionHive().getSparkSession();
		  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
		  JavaRDD<String> pageRDD = sparkHive.sparkContext().textFile(Constants.HDFS_PAGEDATA_YESTERDAY, 3).toJavaRDD();
		    
		   String schemaString = "userid pagename logintime logouttime";

		    // Generate the schema based on the string of schema
		    List<StructField> fields = new ArrayList<StructField>();
		    for (String fieldName : schemaString.split(" ")) {
		      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		      fields.add(field);
		    }
		    
		    StructType schema = DataTypes.createStructType(fields);
		   
		    JavaRDD<Row> rowRDD = pageRDD.map(new Function<String, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String row) throws Exception {
						
						String[] attributes = row.split(",");
						
						if(attributes.length ==5){
							
							return RowFactory.create(attributes[0],attributes[1],attributes[3],attributes[4]);
						}else{
							return RowFactory.create(null,null,null,null);
						}  
					}
			});
		    if(!rowRDD.isEmpty()){
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = sparkHive.createDataFrame(rowRDD, schema);
		    // Creates a temporary view using the DataFram
		    peopleDataFrame.createOrReplaceTempView("t_user_page");
		    
		    sparkHive.sql("select t.userid,"
		    		+ "t.pagename,"
		    		+ "count(t.userid) login_count,"
		    		+ "sum(if (unix_timestamp(logouttime)-unix_timestamp(logintime) < 1800 ,(unix_timestamp(logouttime)-unix_timestamp(logintime)),0 )) as use_time"
		    		+ " FROM (select userid,pagename,logintime,logouttime "
		    		+ " FROM t_user_page "
		    		+ " WHERE logintime >= '2017-01-16' AND logintime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') "
		    		+ " AND logouttime >= '2017-01-16' AND logouttime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')) t"
		    		+ " GROUP by t.userid,t.pagename")
		    	   .createOrReplaceTempView("t_page_use_time");
		    
		    sparkHive.sql("use sdkdata");
		    sparkHive.sql("select a.userid ,"
		    		+ "b.modulename module,"
		    		+ "count(a.userid) pv,"
		    		+ "sum(a.use_time) use_time"
		    		+ " FROM t_page_use_time a , page_module_map b "
		    		+ " WHERE a.pagename=b.pagename "
		    		+ " GROUP by a.userid,b.modulename")
		    		.createOrReplaceTempView("t_user_modules_time");
		    
		    sparkHive.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
		    .createOrReplaceTempView("t_user_base");
		    
		    //需要把这个结构表放到hive里（hdfs上的目录）
		   Dataset<Row> userModuleUseTimeDSet = sparkHive.sql("SELECT "
		   			+ "a.userid,"
		    		+ "b.s_user_name user_name,"
		    		+ "b.s_type user_type,"
		    		+ "b.s_xiaoxincode xiaoxin_code,"
		    		+ "a.module,"
		    		+ "a.pv,"
		    		+ "a.use_time ,"
		    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') report_date "
		    		+ " FROM t_user_modules_time a, t_user_base b "
		    		+ " WHERE  a.userid = b.p_id AND  a.module is not null and a.use_time >=0");
		   //userModuleUseTimeDSet.write().mode(SaveMode.Append).save("/hive_SDKdata/moduletimedata/moduletimedata"+DateUtils.getYesterdayDateyyMM()+".txt");
		   
		   userModuleUseTimeDSet.toDF().createOrReplaceTempView("t_user_module");
		   sparkHive.sql("use sdkdata");
		   sparkHive.sql(" insert into  table moduledata "
		   			+ " select userid,user_name,user_type,xiaoxin_code,module,pv,use_time,report_date from t_user_module");
		   
		   sparkHive.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_REPORT, Constants.JdbcCon())
		    .createOrReplaceTempView("t_user_report");
		   
		   sparkHive.sql("select "
		    		+ "a.userid,"
		    		+ "a.user_type s_type,"
		    		+ "a.module ,"
		    		+ "a.use_time,"
		    		+ "b.f_province_id ,"
		    		+ "b.f_city_id ,"
		    		+ "b.f_area_id  "
		    		+ " FROM t_user_module a,t_user_report b "
		    		+ " WHERE a.userid=b.s_user_id")
		    .createOrReplaceTempView("t_area_user_usetime");
		    
		    Dataset<Row> resultDSet = sparkHive.sql("select "
		    		+ " f_province_id,"
		    		+ " f_city_id,"
		    		+ " f_area_id,"
		    		+ "s_type,"
		    		+ "module s_module_name,"
		    		+ "count(userid) s_user_count,"
		    		+ "sum(use_time) s_use_time ,"
		    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date"
		    		+ " FROM t_area_user_usetime "
		    		+ " GROUP BY f_province_id,f_city_id,f_area_id,s_type,module");
		    resultDSet.write().mode("append").jdbc(Constants.JDBC_EXIAOXIN, Constants.T_REPORT_AREA_MODULE, Constants.JdbcCon());
		    
		    sparkHive.stop();
		    }//if 对rdd.isEmpty的结尾    
		    
	}

}
