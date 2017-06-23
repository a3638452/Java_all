package com.orange.service;

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

import com.orange.bean.UserBaseTags;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.SparkSessionHDFS;
import com.orange.dao.UserBaseTagsDAO;
import com.orange.dao.factory.DAOFactory;

import scala.Tuple2;
/**
 * 查不到的用户要么是省市区有一个为空的，要么大多是游客
 * @author Administrator
 *
 */
public class UserBaseInfoTags  {
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://192.168.0.120:3306/exiaoxin";
	   static final String USER = "xxv2";
	   static final String PASS = "xv2PassWD-321";
	   
	   public static void main(String[] args) {

		//1.构建sparksession
		  SparkSession spark = new SparkSessionHDFS().getSparkSession();
		
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
									  		+ "f_area_id "
		  		+ "FROM t_user_report "
		  		+ "WHERE s_user_type = '0' and f_area_id !='' and f_area_id is not null").persist();
		  Map<String, String> visitMap = visitDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			  
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				String user_type = String.valueOf(row.getInt(1));
				String province = row.getString(2);
				String city = row.getString(3);
				String area = row.getString(4);
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area, "0");
			}
		}).collectAsMap();
		  
//		  for(Entry<String, String> kv:visitMap.entrySet()){
//			  System.out.println(kv.getKey());
//			  System.out.println(kv.getValue());
//		  }
		  //老师
		  Dataset<Row> teacherDSet = spark.sql("SELECT distinct  a.s_user_id,"
										  		+ "a.s_user_type,"
										  		+ "a.f_province_id,"
										  		+ "a.f_city_id,"
										  		+ "a.f_area_id,"
										  		+ "d.s_name " +
					" FROM t_user_report a,t_class_teacher_map b,t_school c,t_sys_dict d "+
					" WHERE a.s_user_id=b.f_user_id AND b.f_school_id=c.p_id AND c.f_school_type=d.p_id "
					+ "AND  a.s_user_type ='2' AND a.f_area_id !='' and a.f_area_id is not null "+
					" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id, d.s_name ").persist();
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
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area, sch_name);
			}
		}).collectAsMap();
		 
		  //家长
		  Dataset<Row> parentDSet = spark.sql("SELECT distinct a.s_user_id,"
									  		+ "a.s_user_type,"
									  		+ "a.f_province_id,"
									  		+ "a.f_city_id,"
									  		+ "a.f_area_id,"
									  		+ "e.s_name "+
					" FROM  t_user_report a,t_student_parent_map b,t_student_class_map c,t_school d,t_sys_dict e "+
					" WHERE a.s_user_id=b.f_par_user_id AND b.f_student_id=c.f_student_id AND c.f_school_id=d.p_id "
					+ "AND d.f_school_type=e.p_id and  a.s_user_type ='1' and a.f_area_id !='' and a.f_area_id is not null "+
					" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id,e.s_name").persist();
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
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area, sch_name);
			}
		}).collectAsMap();
		  
		  //家长亦老师
		  Dataset<Row> parentAlsoDSet = spark.sql("SELECT distinct a.s_user_id, "+
						"a.s_user_type, "+
						"a.f_province_id, "+
						"a.f_city_id, "+
						"a.f_area_id, "+
						"e.s_name  "+
						" FROM  t_user_report a,t_student_parent_map b,t_student_class_map c,t_school d,t_sys_dict e  "+
						" WHERE a.s_user_id=b.f_par_user_id AND b.f_student_id=c.f_student_id AND c.f_school_id=d.p_id "
						+ "AND d.f_school_type=e.p_id AND  a.s_user_type ='3' AND a.f_area_id !='' AND a.f_area_id is not null "+
						" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id,e.s_name  ").persist();
		  
						Dataset<Row> teacherAlsoDSet = spark.sql(" SELECT distinct a.s_user_id, "+
						"a.s_user_type, "+
						"a.f_province_id, "+
						"a.f_city_id, "+
						"a.f_area_id, "+
						"d.s_name  "+
						" FROM t_user_report a,t_class_teacher_map b,t_school c,t_sys_dict d  "+
						" WHERE a.s_user_id=b.f_user_id AND b.f_school_id=c.p_id AND c.f_school_type=d.p_id "
						+ "AND  a.s_user_type ='3' AND a.f_area_id !='' AND a.f_area_id is not null "+
						" GROUP BY a.s_user_id,a.s_user_type,a.f_province_id,a.f_city_id,a.f_area_id, d.s_name").persist();
		  
						
						Dataset<Row> parentAndteacherDSet = parentAlsoDSet.union(teacherAlsoDSet);
						
		  Map<String, String> parentAndteacherMap = parentAndteacherDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			  
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String user_id = row.getString(0);
				String user_type = String.valueOf(row.getInt(1));
				String province = row.getString(2);
				String city = row.getString(3);
				String area = row.getString(4);
				String sch_name = row.getString(5);
				return new Tuple2<String, String>(user_id+","+user_type+","+province+","+city+","+area, sch_name);
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
		      String sql = "truncate table t_user_base_tags";
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
		  reultmap1.putAll(teacherMap);
		  reultmap1.putAll(parentAndteacherMap);
		  
		UserBaseTags userBaseTags = new UserBaseTags();
		  //插入DB
		  for(Entry<String, String> kv:reultmap1.entrySet()){
			
			  String[] split = kv.getKey().split(",");
			  //if(split.length>=5){
			 
			  String user_id = split[0];
			  String user_type = split[1];
			  String province = String.valueOf(split[2]);
			  String city = String.valueOf(split[3]);
			  String area = String.valueOf(split[4]);
			  String s_report_date = DateUtils.getYesterdayDate();
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
			//  }  
		  }
		  
		  
		  spark.stop();
	}

}
