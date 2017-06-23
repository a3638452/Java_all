package com.orange.service;

import java.io.Serializable;
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

import com.orange.bean.DayUseTime;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.dao.DayUseTimeDAO;
import com.orange.dao.factory.DAOFactory;


public class DayUserUseTime implements Serializable{

	private static final long serialVersionUID = 1L;

	public  void dayUserUseTime(SparkSession sparkHDFS) {

		  JavaRDD<String> pageRDD = sparkHDFS.sparkContext().textFile(Constants.HDFS_SYSTIMEDATA_YESTERDAY, 3).toJavaRDD();
		    
		   String schemaString = "userid logintime logouttime";

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
						
						if(attributes.length ==6 && attributes[0].length() <= 40){
							
							return RowFactory.create(attributes[0],attributes[1],attributes[2]);
						}else{
							return RowFactory.create(null,null,null);
						}  
					}
			});
		    if(!rowRDD.isEmpty()){
			    // Apply the schema to the RDD
			    Dataset<Row> peopleDataFrame = sparkHDFS.createDataFrame(rowRDD, schema);

			    // Creates a temporary view using the DataFram
			    peopleDataFrame.createOrReplaceTempView("t_logindata");
			    String str01 = new String();
			    String str02 = new String();
			    String str03 = new String();
			    String str04 = new String();
			    String str05 = new String();
			    String str06 = new String();
			    String str07 = new String();
			    String str08 = new String();
			    String str09 = new String();
			    
			    Dataset<Row> jdbc = sparkHDFS.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_MIDDLE_USETIME, Constants.JdbcCon());
			    List<Row> collect = jdbc.select("group_distance").toJavaRDD().collect();
				  
				   for (Row row : collect) {
					   String[] split = row.toString().split(",");
					   str01 = split[0].substring(1, split[0].length());
					   str02 = split[1];
					   str03 = split[2];
					   str04 = split[3];
					   str05 = split[4];
					   str06 = split[5];
					   str07 = split[6];
					   str08 = split[7];
					   str09 = split[8].substring(0, split[8].length() - 1);
				   }
				   
				   sparkHDFS.sql("select t.userid,"
			    		+ "count(t.userid) login_count,"
			    		+ "sum(if (unix_timestamp(logouttime)-unix_timestamp(logintime) < '1200'  ,(unix_timestamp(logouttime)-unix_timestamp(logintime)),0 )) as use_time"
			    		+ " FROM (select userid,logintime,logouttime "
			    		+ " FROM t_logindata "
			    		+ " WHERE logintime >= '2017-01-16' AND logintime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') "
			    		+ " AND logouttime >= '2017-01-16'  AND logouttime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') ) t"
			    		+ " GROUP by t.userid ")
			      .createOrReplaceTempView("t_use_time");
				  sparkHDFS.sql("SELECT userid, "+
						  "use_time, "+
					 "CASE WHEN use_time>='"+str01+"' and use_time < '"+str02+"' THEN '1,' '"+str01+"' '~' '"+str02+"' '秒' " + 
						  "WHEN use_time>='"+str02+"' and use_time < '"+str03+"' THEN '2,' '"+str02+"' '~' '"+str03+"' '秒' " + 
						  "WHEN use_time>='"+str03+"' and use_time < '"+str04+"' THEN '3,' '"+str03+"' '~' '"+str04+"' '秒' " + 
						  "WHEN use_time>='"+str04+"' and use_time < '"+str05+"' THEN '4,' '"+str04+"' '~' '"+str05+"' '秒' " + 
						  "WHEN use_time>='"+str05+"' and use_time < '"+str06+"' THEN '5,' '"+(Long.valueOf(str05)/60)+"' '~' '"+(Long.valueOf(str06)/60)+"' '分钟' " + 
						  "WHEN use_time>='"+str06+"' and use_time < '"+str07+"' THEN '6,' '"+(Long.valueOf(str06)/60)+"' '~' '"+(Long.valueOf(str07)/60)+"' '分钟' " + 
						  "WHEN use_time>='"+str07+"' and use_time < '"+str08+"' THEN '7,' '"+(Long.valueOf(str07)/60)+"' '~' '"+(Long.valueOf(str08)/60)+"' '分钟' " + 
						  "WHEN use_time>='"+str08+"'  THEN '8,' '"+(Long.valueOf(str08)/60)+"' '分钟' '"+str09+"' "+ 
						 "END AS group_distance "+ 
						  "FROM t_use_time").createOrReplaceTempView("t_use_time_p");
				   
//SELECT * FROM t_report_user_staytime  WHERE report_date ='2017-06-07' ORDER BY FIELD(stay_time,0,3,10,30,90,180,600,1800); 
				   
			   Dataset<Row> resultDataSet = sparkHDFS.sql("select group_distance stay_time,"
								   		+ "count(userid) user_count, "
								   		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') report_date "
								   		+ "from t_use_time_p "
								   		+ "WHERE group_distance is not null and group_distance !='' "
								   		+ "group by group_distance order by (group_distance,1,2,3,4,5,6,7,8)");
			   
			   List<Row> collect2 = resultDataSet.toJavaRDD().collect();
			   DayUseTime dayUseTime = new DayUseTime();
			   for (Row row : collect2) {
				   
				   String stay_time = row.getString(0).split(",")[1];
				   Long user_count = row.getLong(1);
				   
					   dayUseTime.setStay_time(stay_time);
					   dayUseTime.setUser_count(String.valueOf(user_count));
					   dayUseTime.setReport_date(DateUtils.getYesterdayDate());
					   
					   DayUseTimeDAO dayUseTimeDAO = DAOFactory.getDayUseTimeDAO();
					   dayUseTimeDAO.insert(dayUseTime);
				   }
			   }
			   
		    
	}

}
