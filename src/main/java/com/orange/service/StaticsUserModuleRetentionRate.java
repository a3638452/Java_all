package com.orange.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.bean.ModuleUserCountBean;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.SparkSessionHive;
import com.orange.dao.ModuleUserCountDAO;
import com.orange.dao.Update2DayModuleUserCountDAO;
import com.orange.dao.factory.DAOFactory;

import scala.Tuple2;

public class StaticsUserModuleRetentionRate {

	public static void main(String[] args) {
		
		SparkSession sparkHive = new SparkSessionHive().getSparkSession();
		//查询出用户信息表't_user_base'
		Dataset<Row> jdbc = sparkHive.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon());
		  
		sparkHive.sql("use sdkdata");
		Dataset<Row> peopleDataFrame = sparkHive.sql(
					  "SELECT userid,pagename "
					+ "FROM pagedata "
					+ "WHERE  logintime >= FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') "
					+ "AND logintime < FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') ");
		 //2.读取Hive上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
	    
	    //昨天新注册用户访问模块人数
	    getYesterdayRegisterUsers(sparkHive,jdbc,peopleDataFrame);
	    //次日模块留存人数
	    getYesRetentionUsers(sparkHive,jdbc,peopleDataFrame);
	    //3日模块留存人数
	    getThreeRetentionUsers(sparkHive,jdbc,peopleDataFrame);
	    //7日模块留存人数
	    getSevenRetentionUsers(sparkHive,jdbc,peopleDataFrame);
	    //14日模块留存人数
	    getFourteenRetentionUsers(sparkHive,jdbc,peopleDataFrame);
	    //30日模块留存人数
	    getThirtyRetentionUsers(sparkHive,jdbc,peopleDataFrame);
	    
	    //AppendToLogFile.appendFile("\r\n"+"全部执行完毕的标志~！！！" , Constants.LOGURL); 
	    
	   // }
	  sparkHive.stop();
}

	
	
	/**
	 * //昨天的新注册用户 
	 * @param sparkHive
	 * @param jdbc 
	 * @param peopleDataFrame 
	 */
	private static   void getYesterdayRegisterUsers(SparkSession sparkHive, Dataset<Row> jdbc, Dataset<Row> peopleDataFrame) {
		 
		jdbc.createOrReplaceTempView("t_user_base");
		peopleDataFrame.createOrReplaceTempView("t_pagedata");
		
		Dataset<Row> DSet = sparkHive.sql("SELECT p_id "
		  		+ "FROM t_user_base "
		  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') "
		  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')");
		  
		 List<String> yesList = DSet.toJavaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		}).collect();
		 
		 String strips = StringUtils.strip(yesList.toString(),"[]");
		 String strip = strips.replaceAll(", ", "','");
	    
	    //查出新注册用户昨天浏览的页面，返回DSet
	    Dataset<Row> userPageDSet = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strip+"')");
	    //对Dset去重，并变成临时表
	    userPageDSet.distinct().createOrReplaceTempView("t_new_user_page");
	    sparkHive.sql("use sdkdata");
	    Dataset<Row> userModuleDistDSet = sparkHive.sql("SELECT b.modulename,a.userid "
										    		+ "FROM t_new_user_page a,page_module_map b "
										    		+ "WHERE a.pagename=b.pagename");
	    JavaPairRDD<String, String> pairUserModuleDistDSet = userModuleDistDSet.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {

				return new Tuple2<String, String>(row.getString(0), row.getString(1));
			}
		});
	    
	    //※reduceByKey后的map，现在的结果是去重后的<module,count(userId)>
	    Map<String, Long> moduleCountUserMapYes = pairUserModuleDistDSet.countByKey();
	   
	    ModuleUserCountBean moduleUserCountBean = new ModuleUserCountBean();
	    for (Entry<String, Long> map : moduleCountUserMapYes.entrySet()) {
		   String module = map.getKey();
		   String user_count = String.valueOf(map.getValue());
		   
		   moduleUserCountBean.setS_report_date(DateUtils.getYesterdayDate());
		   moduleUserCountBean.setModule(module);
		   moduleUserCountBean.setUser_count(user_count);
		   
		   //AppendToLogFile.appendFile("\r\n"+"昨天的新注册用户模块使用情况module:" +module + "    user_count:"+user_count +"   时间："+DateUtils.getPre2dayDate(), Constants.LOGURL);
		   
		   ModuleUserCountDAO moduleUserCountDAO = DAOFactory.getModuleUserCountDAO();
		   moduleUserCountDAO.insert(moduleUserCountBean); 
	    }
	}
	
	    /**
	     * 次日留存人数
	     */
	    private static void getYesRetentionUsers(SparkSession sparkHive, Dataset<Row> jdbc, Dataset<Row> peopleDataFrame) {
	    	
	    	jdbc.createOrReplaceTempView("t_user_base");
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			
			  //2天前刚注册的用户
			  Dataset<Row> DSet2 = sparkHive.sql("SELECT p_id "
			  		+ "FROM t_user_base "
			  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-172800,'yyyy-MM-dd') "
			  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd')");
			                  
			 List<String> twoDayList = DSet2.toJavaRDD().map(new Function<Row, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			}).collect();
			 
			 String strip2 = StringUtils.strip(twoDayList.toString(),"[]");
			 String strips2 = strip2.replaceAll(", ", "','");
			
			    //查出2天前刚注册的用户在昨天访问页面的记录
			    Dataset<Row> userPageDSet2 = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strips2+"')");
			    //对Dset去重，并变成临时表
			    userPageDSet2.distinct().createOrReplaceTempView("t_pre2day_user_page");
			    
			    Dataset<Row> userModuleDistDSet2 = sparkHive.sql("SELECT b.modulename,a.userid "
												    		+ "FROM t_pre2day_user_page a,page_module_map b "
												    		+ "WHERE a.pagename=b.pagename");
			    JavaPairRDD<String, String> pairUserModuleDistDSet2 = userModuleDistDSet2.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {

						return new Tuple2<String, String>(row.getString(0), row.getString(1));
					}
				});
			    
			    //2天前的注册用户在昨天访问页面的<module,count(userid)>Map
			    Map<String, Long> userModuleDistMap2Day = pairUserModuleDistDSet2.countByKey();
			    
			    //对2天前注册用户的留存进行更新添加
			    ArrayList<ModuleUserCountBean> arrayList = new ArrayList<>();
			    ModuleUserCountBean moduleUserCountBean2 = new ModuleUserCountBean();
			    for (Entry<String, Long> map : userModuleDistMap2Day.entrySet()) {
			    	
			    	String module = map.getKey();
			    	String user_count = String.valueOf(map.getValue());
			    	
			    	moduleUserCountBean2.setNext_day(user_count);
			    	moduleUserCountBean2.setModule(module);
			    	moduleUserCountBean2.setS_report_date(DateUtils.getPre2dayDate());
			    	arrayList.add(moduleUserCountBean2);
			    	//AppendToLogFile.appendFile("\r\n"+"2天前注册的用户在昨天也使用相同的模块module:" +module +"     Next_day:"+user_count + "       reportdate:"+DateUtils.getPre2dayDate(), Constants.LOGURL);
			    	 
			    	//执行批量更新2天前的日期模块用户量
			    	Update2DayModuleUserCountDAO update2DayModuleUserCountDAO = DAOFactory.getUpdate2DayModuleUserCountDAO();
			    	update2DayModuleUserCountDAO.updateBatch(arrayList);
			    	}
	}
	    
	    /**
	     * 3日留存人数
	     * @param sparkHive
	     * @param jdbc
	     * @param peopleDataFrame
	     */
	    private static void getThreeRetentionUsers(SparkSession sparkHive, Dataset<Row> jdbc,
				Dataset<Row> peopleDataFrame) {
	    	
	    	jdbc.createOrReplaceTempView("t_user_base");
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			
	    	//4天前刚注册的用户
			  Dataset<Row> DSet3 = sparkHive.sql("SELECT p_id "
			  		+ "FROM t_user_base "
			  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-345600,'yyyy-MM-dd') "
			  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP()-259200,'yyyy-MM-dd')");
			                  
			 List<String> threeDayList = DSet3.toJavaRDD().map(new Function<Row, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			}).collect();
			 
			 String strip3 = StringUtils.strip(threeDayList.toString(),"[]");
			 String strips3 = strip3.replaceAll(", ", "','");
			
			    //查出4天前刚注册的用户在昨天访问页面的记录
			    Dataset<Row> userPageDSet3 = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strips3+"')");
			    //对Dset去重，并变成临时表
			    userPageDSet3.distinct().createOrReplaceTempView("t_pre2day_user_page");
			    
			    Dataset<Row> userModuleDistDSet3 = sparkHive.sql("SELECT b.modulename,a.userid "
												    		+ "FROM t_pre2day_user_page a,page_module_map b "
												    		+ "WHERE a.pagename=b.pagename");
			    JavaPairRDD<String, String> pairUserModuleDistDSet3 = userModuleDistDSet3.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {

						return new Tuple2<String, String>(row.getString(0), row.getString(1));
					}
				});
			    
			    //4天前的注册用户在昨天访问页面的<module,count(userid)>Map
			    Map<String, Long> userModuleDistMap3Day = pairUserModuleDistDSet3.countByKey();
			    
			    //对4天前注册用户的留存进行更新添加
			    ArrayList<ModuleUserCountBean> List3 = new ArrayList<>();
			    ModuleUserCountBean moduleUserCountBean3 = new ModuleUserCountBean();
			    for (Entry<String, Long> map : userModuleDistMap3Day.entrySet()) {
			    	
			    	String module = map.getKey();
			    	String user_count = String.valueOf(map.getValue());
			    	
			    	moduleUserCountBean3.setThree_day(user_count);
			    	moduleUserCountBean3.setModule(module);
			    	moduleUserCountBean3.setS_report_date(DateUtils.getPre4dayDate());
			    	List3.add(moduleUserCountBean3);
			    	//AppendToLogFile.appendFile("\r\n"+"4天前注册的用户在昨天也使用相同的模块module:" +module +"     Next_day:"+user_count + "       reportdate:"+DateUtils.getPre2dayDate(), Constants.LOGURL);
			    	 
			    	//执行批量更新2天前的日期模块用户量
			    	Update2DayModuleUserCountDAO update2DayModuleUserCountDAO = DAOFactory.getUpdate2DayModuleUserCountDAO();
			    	update2DayModuleUserCountDAO.updateBatch3(List3);
			    	}
			
		}

	    /**
	     * 7日留存人数
	     * @param sparkHive
	     * @param jdbc
	     * @param peopleDataFrame
	     */
	    private static void getSevenRetentionUsers(SparkSession sparkHive, Dataset<Row> jdbc,
				Dataset<Row> peopleDataFrame) {
			
	    	jdbc.createOrReplaceTempView("t_user_base");
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			
	    	//8天前刚注册的用户
			  Dataset<Row> DSet7 = sparkHive.sql("SELECT p_id "
			  		+ "FROM t_user_base "
			  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-691200,'yyyy-MM-dd') "
			  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP()-604800,'yyyy-MM-dd')");
			                  
			 List<String> sevenDayList = DSet7.toJavaRDD().map(new Function<Row, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			}).collect();
			 
			 String strip7 = StringUtils.strip(sevenDayList.toString(),"[]");
			 String strips7 = strip7.replaceAll(", ", "','");
			
			    //查出8天前刚注册的用户在昨天访问页面的记录
			    Dataset<Row> userPageDSet7 = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strips7+"')");
			    //对Dset去重，并变成临时表
			    userPageDSet7.distinct().createOrReplaceTempView("t_pre2day_user_page");
			    
			    Dataset<Row> userModuleDistDSet7 = sparkHive.sql("SELECT b.modulename,a.userid "
												    		+ "FROM t_pre2day_user_page a,page_module_map b "
												    		+ "WHERE a.pagename=b.pagename");
			    JavaPairRDD<String, String> pairUserModuleDistDSet7 = userModuleDistDSet7.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {

						return new Tuple2<String, String>(row.getString(0), row.getString(1));
					}
				});
			    
			    //8天前的注册用户在昨天访问页面的<module,count(userid)>Map
			    Map<String, Long> userModuleDistMap7Day = pairUserModuleDistDSet7.countByKey();
			    
			    //对8天前注册用户的留存进行更新添加
			    ArrayList<ModuleUserCountBean> List7 = new ArrayList<>();
			    ModuleUserCountBean moduleUserCountBean7 = new ModuleUserCountBean();
			    for (Entry<String, Long> map : userModuleDistMap7Day.entrySet()) {
			    	
			    	String module = map.getKey();
			    	String user_count = String.valueOf(map.getValue());
			    	
			    	moduleUserCountBean7.setSeven_day(user_count);
			    	moduleUserCountBean7.setModule(module);
			    	moduleUserCountBean7.setS_report_date(DateUtils.getPre8dayDate());
			    	List7.add(moduleUserCountBean7);
			    	//AppendToLogFile.appendFile("\r\n"+"4天前注册的用户在昨天也使用相同的模块module:" +module +"     Next_day:"+user_count + "       reportdate:"+DateUtils.getPre2dayDate(), Constants.LOGURL);
			    	 
			    	//执行批量更新8天前的日期模块用户量
			    	Update2DayModuleUserCountDAO update7DayModuleUserCountDAO = DAOFactory.getUpdate2DayModuleUserCountDAO();
			    	update7DayModuleUserCountDAO.updateBatch7(List7);
			    	}
		}
	    
	    /**
	     * 14日留存人数
	     * @param sparkHive
	     * @param jdbc
	     * @param peopleDataFrame
	     */
	    private static void getFourteenRetentionUsers(SparkSession sparkHive, Dataset<Row> jdbc,
				Dataset<Row> peopleDataFrame) {
			
	    	jdbc.createOrReplaceTempView("t_user_base");
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			
	    	//15天前刚注册的用户
			  Dataset<Row> DSet14 = sparkHive.sql("SELECT p_id "
			  		+ "FROM t_user_base "
			  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-1296000,'yyyy-MM-dd') "
			  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP()-1209600,'yyyy-MM-dd')");
			                  
			 List<String> fourteenDayList = DSet14.toJavaRDD().map(new Function<Row, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			}).collect();
			 
			 String strip14 = StringUtils.strip(fourteenDayList.toString(),"[]");
			 String strips14 = strip14.replaceAll(", ", "','");
			
			    //查出15天前刚注册的用户在昨天访问页面的记录
			    Dataset<Row> userPageDSet14 = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strips14+"')");
			    //对Dset去重，并变成临时表
			    userPageDSet14.distinct().createOrReplaceTempView("t_pre2day_user_page");
			    
			    Dataset<Row> userModuleDistDSet14 = sparkHive.sql("SELECT b.modulename,a.userid "
												    		+ "FROM t_pre2day_user_page a,page_module_map b "
												    		+ "WHERE a.pagename=b.pagename");
			    JavaPairRDD<String, String> pairUserModuleDistDSet14 = userModuleDistDSet14.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {

						return new Tuple2<String, String>(row.getString(0), row.getString(1));
					}
				});
			    
			    //15天前的注册用户在昨天访问页面的<module,count(userid)>Map
			    Map<String, Long> userModuleDistMap14Day = pairUserModuleDistDSet14.countByKey();
			    
			    //对15天前注册用户的留存进行更新添加
			    ArrayList<ModuleUserCountBean> List14 = new ArrayList<>();
			    ModuleUserCountBean moduleUserCountBean14 = new ModuleUserCountBean();
			    for (Entry<String, Long> map : userModuleDistMap14Day.entrySet()) {
			    	
			    	String module = map.getKey();
			    	String user_count = String.valueOf(map.getValue());
			    	
			    	moduleUserCountBean14.setFourteen_day(user_count);
			    	moduleUserCountBean14.setModule(module);
			    	moduleUserCountBean14.setS_report_date(DateUtils.getPre15dayDate());
			    	List14.add(moduleUserCountBean14);
			    	//AppendToLogFile.appendFile("\r\n"+"4天前注册的用户在昨天也使用相同的模块module:" +module +"     Next_day:"+user_count + "       reportdate:"+DateUtils.getPre2dayDate(), Constants.LOGURL);
			    	 
			    	//执行批量更新2天前的日期模块用户量
			    	Update2DayModuleUserCountDAO update14DayModuleUserCountDAO = DAOFactory.getUpdate2DayModuleUserCountDAO();
			    	update14DayModuleUserCountDAO.updateBatch14(List14);
			    	}
		}
	    
	    /**
	     * 30日留存人数
	     * @param sparkHive
	     * @param jdbc
	     * @param peopleDataFrame
	     */
	    private static void getThirtyRetentionUsers(SparkSession sparkHive, Dataset<Row> jdbc,
				Dataset<Row> peopleDataFrame) {
			
	    	jdbc.createOrReplaceTempView("t_user_base");
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			
	    	//31天前刚注册的用户
			  Dataset<Row> DSet30 = sparkHive.sql("SELECT p_id "
			  		+ "FROM t_user_base "
			  		+ "WHERE s_register_time >= FROM_UNIXTIME(UNIX_TIMESTAMP()-2678400,'yyyy-MM-dd') "
			  		+ "AND s_register_time < FROM_UNIXTIME(UNIX_TIMESTAMP()-2592000,'yyyy-MM-dd')");
			                  
			 List<String> thirtyDayList = DSet30.toJavaRDD().map(new Function<Row, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			}).collect();
			 
			 String strip30 = StringUtils.strip(thirtyDayList.toString(),"[]");
			 String strips30 = strip30.replaceAll(", ", "','");
			
			    //查出31天前刚注册的用户在昨天访问页面的记录
			    Dataset<Row> userPageDSet30 = sparkHive.sql("select userid,pagename from t_pagedata where userid in('"+strips30+"')");
			    //对Dset去重，并变成临时表
			    userPageDSet30.distinct().createOrReplaceTempView("t_pre2day_user_page");
			    
			    Dataset<Row> userModuleDistDSet30 = sparkHive.sql("SELECT b.modulename,a.userid "
												    		+ "FROM t_pre2day_user_page a,page_module_map b "
												    		+ "WHERE a.pagename=b.pagename");
			    JavaPairRDD<String, String> pairUserModuleDistDSet30 = userModuleDistDSet30.distinct().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Row row) throws Exception {

						return new Tuple2<String, String>(row.getString(0), row.getString(1));
					}
				});
			    
			    //31天前的注册用户在昨天访问页面的<module,count(userid)>Map
			    Map<String, Long> userModuleDistMap30Day = pairUserModuleDistDSet30.countByKey();
			    
			    //对31天前注册用户的留存进行更新添加
			    ArrayList<ModuleUserCountBean> List30 = new ArrayList<>();
			    ModuleUserCountBean moduleUserCountBean30 = new ModuleUserCountBean();
			    for (Entry<String, Long> map : userModuleDistMap30Day.entrySet()) {
			    	
			    	String module = map.getKey();
			    	String user_count = String.valueOf(map.getValue());
			    	
			    	moduleUserCountBean30.setThirty_day(user_count);
			    	moduleUserCountBean30.setModule(module);
			    	moduleUserCountBean30.setS_report_date(DateUtils.getPre31dayDate());
			    	List30.add(moduleUserCountBean30);
			    	//AppendToLogFile.appendFile("\r\n"+"4天前注册的用户在昨天也使用相同的模块module:" +module +"     Next_day:"+user_count + "       reportdate:"+DateUtils.getPre2dayDate(), Constants.LOGURL);
			    	 
			    	//执行批量更新2天前的日期模块用户量
			    	Update2DayModuleUserCountDAO update7DayModuleUserCountDAO = DAOFactory.getUpdate2DayModuleUserCountDAO();
			    	update7DayModuleUserCountDAO.updateBatch30(List30);
			    	}
		}

}
