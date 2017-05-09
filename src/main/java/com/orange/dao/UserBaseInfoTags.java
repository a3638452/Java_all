package com.orange.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import com.orange.bean.UserBaseTags;
import com.orange.common.util.Constants;
import com.orange.common.util.SparkSessionHDFS;
import com.orange.dao.factory.DAOFactory;

public class UserBaseInfoTags implements Serializable{
	private static final long serialVersionUID = 1L;
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://192.168.0.120:3306/exiaoxin";

	   //  Database credentials
	   static final String USER = "xxv2";
	   static final String PASS = "xv2PassWD-321";
	public  void userBaseInfoTags() {

		//1.构建sparksession
		  SparkSession spark = new SparkSessionHDFS().getSparkSession();
		
		  //连续活跃用户连续月活2周(14天),结果验证正确  
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_REPORT, Constants.JdbcCon())
		  .createOrReplaceTempView("t_user_report");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_CLASS_TEACHER_MAP,  Constants.JdbcCon())
		  .createOrReplaceTempView("t_class_teacher_map");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_STUDENT_PARENT_MAP, Constants.JdbcCon())
		  .createOrReplaceTempView("t_student_parent_map");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_STUDENT_CLASS_MAP, Constants.JdbcCon())
		  .createOrReplaceTempView("t_student_class_map");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_SCHOOL, Constants.JdbcCon())
		  .createOrReplaceTempView("t_school");
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_SYS_DICT, Constants.JdbcCon())
		  .createOrReplaceTempView("t_sys_dict");
		  //游客
		  Dataset<Row> visitDSet = spark.sql("SELECT s_user_id,"
									  		+ "s_user_type,"
									  		+ "f_province_id,"
									  		+ "f_city_id,"
									  		+ "f_area_id,"
									  		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date "
		  		+ "FROM t_user_report "
		  		+ "WHERE s_user_type = '0'");
		  Map<String, String> visitMap = visitDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			  
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				String user_type = String.valueOf(row.getInt(1));
				String province = row.getString(2);
				String city = row.getString(3);
				String area = row.getString(4);
				String s_report_date = row.getString(5);
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area+","+s_report_date, "0");
			}
		}).collectAsMap();
		  //老师
		  Dataset<Row> teacherDSet = spark.sql("SELECT  a.s_user_id,"
										  		+ "a.s_user_type,"
										  		+ "a.f_province_id,"
										  		+ "a.f_city_id,"
										  		+ "a.f_area_id,"
										  		+ "d.s_name,"
										  		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date "+
					" FROM t_user_report a,t_class_teacher_map b,t_school c,t_sys_dict d "+
					" WHERE ( a.s_user_type ='2' OR a.s_user_type ='3') AND a.s_user_id=b.f_user_id AND b.f_school_id=c.p_id AND c.f_school_type=d.p_id "+
					" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id, d.s_name ");
		  Map<String, String> teacherMap = teacherDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			  
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				String user_type = String.valueOf(row.getInt(1));
				String province = row.getString(2);
				String city = row.getString(3);
				String area = row.getString(4);
				String sch_name = row.getString(5);
				String s_report_date = row.getString(6);
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area+","+s_report_date, sch_name);
			}
		}).collectAsMap();
		  //家长
		  Dataset<Row> parentDSet = spark.sql("SELECT a.s_user_id,"
									  		+ "a.s_user_type,"
									  		+ "a.f_province_id,"
									  		+ "a.f_city_id,"
									  		+ "a.f_area_id,"
									  		+ "e.s_name,"
									  		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date "+
					" FROM  t_user_report a,t_student_parent_map b,t_student_class_map c,t_school d,t_sys_dict e "+
					" WHERE ( a.s_user_type ='1' OR a.s_user_type ='3') AND a.s_user_id=b.f_par_user_id AND b.f_student_id=c.f_student_id AND c.f_school_id=d.p_id AND d.f_school_type=e.p_id "+
					" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id,e.s_name");
		  Map<String, String> parentMap = parentDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			  
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				String user_type = String.valueOf(row.getInt(1));
				String province = row.getString(2);
				String city = row.getString(3);
				String area = row.getString(4);
				String sch_name = row.getString(5);
				String s_report_date = row.getString(6);
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area+","+s_report_date, sch_name);
			}
		}).collectAsMap();
		  
		  Connection conn = null;
		   Statement stmt = null;
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName(JDBC_DRIVER);
		      //STEP 3: Open a connection
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();
		      String sql = "delete from t_user_base_tags";
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
		  
		  HashMap<String, String> reultmap1 = new HashMap<String,String>();
		  reultmap1.putAll(visitMap);
		  reultmap1.putAll(parentMap);
		  HashMap<String, String> reultmap2 = new HashMap<String,String>();
		  reultmap2.putAll(teacherMap);
		  
		  for(String key:reultmap1.keySet()){
			   if(reultmap2.containsKey(key) && !reultmap2.get(key).contains(reultmap1.get(key))){
				   reultmap2.put(key, reultmap1.get(key)+","+reultmap2.get(key));
			   }else{
				   reultmap2.put(key, reultmap1.get(key));
			   }
			  }
		UserBaseTags userBaseTags = new UserBaseTags();
		  //插入DB
		  for(Entry<String, String> kv:reultmap2.entrySet()){
			  String[] split = kv.getKey().split(",");
			  String user_id = split[0];
			  String user_type = split[1];
			  String province = split[2];
			  String city = split[3];
			  String area = split[4];
			  String s_report_date = split[5];
			  String sch_name = kv.getValue();
			  
			  userBaseTags.setS_user_id(user_id);
			  userBaseTags.setS_user_type(user_type);
			  userBaseTags.setF_province_id(province);
			  userBaseTags.setF_city_id(city);
			  userBaseTags.setF_area_id(area);
			  userBaseTags.setS_school_type(sch_name);
			  userBaseTags.setS_report_date(s_report_date);
			//执行插入方法
			UserBaseTagsDAO userBaseTagsDAO = DAOFactory.getUserBaseTagsDAO();
			userBaseTagsDAO.insert(userBaseTags); 
			  
		  }
		  
		  
		  spark.stop();
	}

}
