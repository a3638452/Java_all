package com.orange.service;


import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.bean.UserLifeCycle;
import com.orange.common.util.Constants;
import com.orange.dao.UserLifeCycleDAO;
import com.orange.dao.factory.DAOFactory;

import scala.Tuple2;

/**
 * 统计用户详细信息、登陆次数和一天中的访问时长
 * @author Administrator
 */
public class StatisticsUserInforLoginTime implements Serializable{
	
	private static final long serialVersionUID = 1L;
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://192.168.0.120:3306/exiaoxin";

	   //  Database credentials
	   static final String USER = "xxv2";
	   static final String PASS = "xv2PassWD-321";
	   public  void statisticsUserInforLoginTime(SparkSession sparkHDFS) {
		
	  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
	  
	  JavaRDD<String> pageRDD = sparkHDFS.sparkContext().textFile(Constants.HDFS_SYSTIMEDATA_YESTERDAY, 3).toJavaRDD();
	    
	   String schemaString = "userid logintime logouttime";

	    // Generate the schema based on the string of schema
	    List<StructField> fields = new ArrayList<StructField>();
	    //String[] attr = new String[4];
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
					
					if(attributes.length ==6){
						
						//String[] attr = attributes;
						
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
	    
	    sparkHDFS.sql("select t.userid,"
	    		+ "count(t.userid) login_count,"
	    		+ "sum(if (unix_timestamp(logouttime)-unix_timestamp(logintime) < '3600'  ,(unix_timestamp(logouttime)-unix_timestamp(logintime)),0 )) as use_time"
	    		+ " FROM (select userid,logintime,logouttime "
	    		+ " FROM t_logindata "
	    		+ " WHERE logintime >= '2017-01-16' AND logintime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') "
	    		+ " AND logouttime >= '2017-01-16'  AND logouttime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')) t"
	    		+ " GROUP by t.userid ")
	    		.createOrReplaceTempView("t_user_count_time");
	    //查询出用户信息表't_user_base'
	    sparkHDFS.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
	    		.createOrReplaceTempView("t_user_base");
    
	    Dataset<Row> userLoginUserTimeRDD = sparkHDFS.sql("SELECT a.userid,"
	    		+ "a.login_count,"
	    		+ "a.use_time,"
	    		+ "b.s_xiaoxincode,"
	    		+ "b.s_account,"
	    		+ "b.s_register_time,"
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_create_time, "
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date, "
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_update_time "
	    		+ " FROM t_user_count_time a,t_user_base b"
	    		+ " WHERE a.userid=b.p_id and a.use_time is not null");
	    userLoginUserTimeRDD.createOrReplaceTempView("t_today_result");
	    //@@当前计算出来的数据集合userLoginUserTimeMap@@
	    Map<String, String> userLoginUserTimeMap = userLoginUserTimeRDD.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				Long login_count = row.getLong(1);
				Long use_time = row.getLong(2);
				String s_xiaoxincode = row.getString(3);
				String s_account = row.getString(4);
				String s_register_time = String.valueOf(row.getTimestamp(5));
				String s_create_time = row.getString(6);
				String s_report_date = row.getString(7);
				String s_update_time = row.getString(8);
				return new Tuple2<String, String>(user_id, login_count+","+use_time+","+s_xiaoxincode+","+s_account+","+s_register_time+","+s_create_time+","+s_report_date+","+s_update_time);
			}
		})
		.collectAsMap();
	    
	  //@@查询出DB中的旧的Map
	    sparkHDFS.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USRE_LIFE_CYCLE, Constants.JdbcCon())
		 .createOrReplaceTempView("t_history");
		 Map<String, String> DBMap = sparkHDFS.sql("select "
		 		+ "f_user_id,"
		 		+ "s_login_count,"
		 		+ "s_use_time,"
		 		+ "s_xiaoxincode,"
		 		+ "s_account,"
		 		+ "s_register_time,"
		 		+ "s_create_time,"
		 		+ "s_report_date, "
		 		+ "s_update_time "
		 		+ " from t_history")
		 		.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {
						String user_id = row.getString(0);
						Long login_count = row.getLong(1);
						Long use_time = row.getLong(2);
						String s_xiaoxincode = row.getString(3);
						String s_account = row.getString(4);
						String s_register_time = row.getString(5);
						String s_create_time = row.getString(6);
						String s_report_date = row.getString(7);
						String s_update_time = row.getString(8);
						return new Tuple2<String, String>(user_id, login_count+","+use_time+","+s_xiaoxincode+","+s_account+","+s_register_time+","+s_create_time+","+s_report_date+","+s_update_time);
					}
				}).collectAsMap();
				
				//@@查询出DB中的新旧相同更新完毕的Map@@
		 Map<String, String> UpdateMap = sparkHDFS.sql("SELECT "
		 		+ "distinct a.f_user_id,"
		 		+ "a.s_login_count+b.login_count login_count,"
		 		+ "a.s_use_time+b.use_time use_time,"
		 		+ "a.s_xiaoxincode,"
		 		+ "a.s_account,"
		 		+ "a.s_register_time, "
		 		+ "a.s_create_time, "
		 		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date, "
		 		+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_update_time "
		 		+ "FROM t_history a,t_today_result b "
		 		+ "WHERE a.f_user_id=b.userid")
		 		.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {
						String user_id = row.getString(0);
						Long login_count = row.getLong(1);
						Long use_time = row.getLong(2);
						String s_xiaoxincode = row.getString(3);
						String s_account = row.getString(4);
						String s_register_time = row.getString(5);
						String s_create_time = row.getString(6);
						String s_report_date = row.getString(7);
						String s_update_time = row.getString(8);
						return new Tuple2<String, String>(user_id, login_count+","+use_time+","+s_xiaoxincode+","+s_account+","+s_register_time+","+s_create_time+","+s_report_date+","+s_update_time);
					}
				})
				.collectAsMap();
		   
		 //相同userid的会把前边的覆盖
		 HashMap<String, String> resultMap = new HashMap<>();
		 resultMap.putAll(DBMap); //DB中历史的
		 resultMap.putAll(userLoginUserTimeMap);  //当前计算出来的昨天最新的
		 resultMap.putAll(UpdateMap);     //DB中历史的和最新的重叠的
		 
		 UserLifeCycle userLifeCycle = new UserLifeCycle();
		 
		 Connection conn = null;
		   Statement stmt = null;
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName(JDBC_DRIVER);
		      //STEP 3: Open a connection
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();
		      String sql = "truncate table t_report_user_login_usetime";
		      stmt.executeUpdate(sql);
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally{
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            conn.close();
		         stmt.close();
		      }catch(SQLException se){
		      }// do nothing
		      try{
		         if(conn!=null)
		            conn.close();
		         stmt.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }//end finally try
		   }//end try
		   
		 for(Entry<String, String> kv:resultMap.entrySet()){
			 String user_id = kv.getKey();
			 if(user_id.length()==36){
			 String[] split = kv.getValue().split(",");
			 Long login_count = Long.valueOf(split[0]);
			 Long use_time = Long.valueOf(split[1]);
			 String s_xiaoxincode = split[2];
			 String s_account = split[3];
			 String s_register_time = split[4];
			 String s_create_time = split[5];
			 String s_report_date = split[6];
			 String s_update_time = split[7];
			 
				//放入bean
				userLifeCycle.setF_user_id(user_id);
				userLifeCycle.setS_xiaoxincode(s_xiaoxincode);
				userLifeCycle.setS_account(s_account);
				userLifeCycle.setS_login_count(login_count);
				userLifeCycle.setS_use_time(use_time);
				userLifeCycle.setS_register_time(s_register_time);
				userLifeCycle.setS_create_time(s_create_time);
				userLifeCycle.setS_report_date(s_report_date);
				userLifeCycle.setS_update_time(s_update_time);
				//执行插入方法
				UserLifeCycleDAO userLifeCycleDAO = DAOFactory.getUserLifeCycleDAO();
				userLifeCycleDAO.insert(userLifeCycle); 
			 }
			 
		 }
		   
		
	  
	    }//if的外层
}

}