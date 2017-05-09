package com.orange.dao;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import com.orange.bean.UserAactiveFrequency;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.SparkSessionHive;
import com.orange.dao.factory.DAOFactory;


/**
 * 每周一运行的，连续活跃、忠诚用户、本周回流、近期流失用户数，按照地区统计
 * @author Administrator
 *
 */
public class StaticsUserActiveFrequency implements Serializable{

	private static final long serialVersionUID = 1L;

	public  void staticsUserActiveFrequency() {

		//1.构建sparksession
		  SparkSession spark = new SparkSessionHive().getSparkSession();
		
		  //连续活跃用户连续月活2周(14天),结果验证正确  
		  spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_REPORT, Constants.JdbcCon())
		  .createOrReplaceTempView("t_user_report");
		  spark.sql("SELECT "
		  		+ " a.userid,"
		  		+ " a.logintime,"
		  		+ " b.f_province_id province,"
		  		+ " b.f_city_id city,"
		  		+ " b.f_area_id  area "
		  		+ " FROM sdkdata.logindata a LEFT JOIN t_user_report b "
		  		+ " ON a.userid=b.s_user_id ")
		  		.createOrReplaceTempView("t_logindata");
		  
		  
		  Dataset<Row> continueActiveUserDset = spark.sql( "SELECT province f_province_id,"
		  		+ "city f_city_id,"
		  		+ "AREA f_area_id,"
		  		+ "COUNT(userid) s_continue_active "
			 +" FROM(SELECT province,city,AREA,userid,DATE_SUB(dt,cn) dts,COUNT(userid) dcn   "
			 +" FROM (SELECT t.*, row_number() over(PARTITION BY userid ORDER BY dt ASC) cn  "
			 +" FROM (SELECT province,city,area,userid, to_date(logintime) dt  "
			 +" FROM t_logindata  "
			 +" WHERE logintime >= FROM_UNIXTIME(UNIX_TIMESTAMP()-1209600,'yyyy-MM-dd') "
			 +" GROUP BY province,city,area,userid, to_date(logintime)) t) s "
			 +" GROUP BY province,city,area,userid,DATE_SUB(dt,cn) "
			 +"  HAVING dcn >= 14) e "
			 +" GROUP BY  province,city,area");
		 // continueActiveUserDset.write().mode("append").jdbc(Constants.JDBC_DBLELE, Constants.T_USER_CONTINUE_ACTIVE, Constants.JdbcCon());
		  /**
		   * 连续活跃用户Map<[省,市,区],user_count>
		   * key: 河北省,廊坊市,文安县 value: 2
		   * key: 广西壮族自治区,南宁市,西乡塘区value: 8
		   */
		  Map<String, String> continueActiveUserMap = continueActiveUserDset.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Row row) throws Exception {
					String province = row.getString(0);
					String city = row.getString(1);
					String area = row.getString(2);
					String user_count = String.valueOf(row.getLong(3));
					return new Tuple2<String, String>(province + "," + city + "," + area, "A_" + user_count);
				}
			})
			.reduceByKeyLocally(new Function2<String, String, String>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {

					return v1+v2;
				}
			});
		  
		  
		   //忠诚用户连续登陆12周(84天)及以上的用户,结果正确
		   Dataset<Row> loyalUserDSet = spark.sql("select province f_province_id,"
									  		    + "city f_city_id,"
									  		    + "area f_area_id,"
										   		+ "count(userid) s_loyal_user "
		   	+" FROM (select province,city,area,userid,week-cn week_cha ,count(userid) cha_count"
   			+" FROM (select t.*, row_number() over(partition by userid order by WEEK asc) cn"
   			+" FROM (SELECT province,city,area,userid,CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-7257600,'yyyy-MM-dd'))/7) AS INT) AS WEEK" 
			+" FROM t_logindata "
			+" WHERE  logintime >= FROM_UNIXTIME(UNIX_TIMESTAMP()-7257600,'yyyy-MM-dd') " 
			+" GROUP BY CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-7257600,'yyyy-MM-dd'))/7) AS INT),province,city,area,userid) t) s"
			+" group by province,city,area,userid,week-cn having cha_count >= 12) x "
			+" group by province,city,area");
		  // loyalUserDSet.write().mode("append").jdbc(Constants.JDBC_DBLELE, Constants.T_USER_LOYAL_USER, Constants.JdbcCon());
		   /**
			   * 忠诚用户Map<[省,市,区],user_count>   
			   * key: 河北省,廊坊市,文安县 value: 2
			   * key: 广西壮族自治区,南宁市,西乡塘区value: 5
			   */
		   Map<String, String> loyalUserDMap = loyalUserDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Row row) throws Exception {
					String province = row.getString(0);
					String city = row.getString(1);
					String area = row.getString(2);
					String user_count = String.valueOf(row.getLong(3));
					return new Tuple2<String, String>(province + "," + city + "," + area, "B_" + user_count);
				}
			})
			.reduceByKeyLocally(new Function2<String, String, String>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {

					return v1+v2;
				}
			});
		 
		   
		    
		    //本周回流,上周没登陆，本周登陆了,结果正确
		    Dataset<Row> weekBackDSet = spark.sql("select province f_province_id,"
										  	    + "city f_city_id,"
										  	    + "area f_area_id,"
									    		+ "count(userid) s_backflow "
		    		+" from (select province,city,area,userid,count(week_cha) user_count "
		    		+" from (select province,city,area,userid,week,week-cn week_cha from (select t.*, row_number() over(partition by userid order by WEEK asc) cn"
		    		+" from (SELECT province,city,area,userid,CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-1209600,'yyyy-MM-dd '))/7) AS INT) AS WEEK" 
					+" FROM t_logindata "
					+" WHERE  logintime >= FROM_UNIXTIME(UNIX_TIMESTAMP()-1209600,'yyyy-MM-dd ') " 
					+" GROUP BY CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-1209600,'yyyy-MM-dd '))/7) AS INT),province,city,area,userid) t) s "
					+" group by province,city,area,userid,week,week-cn "
					+" having week_cha = '0') x "
					+" group by province,city,area,userid) y"
					+" group by province,city,area");
			//weekBackDSet.write().mode("append").jdbc(Constants.JDBC_DBLELE, Constants.T_USER_BACKFLOW, Constants.JdbcCon());
		    /**
			   * 本周回流Map<[省,市,区],user_count>   
			   * key: 河北省,廊坊市,文安县 value: 1
			   * key: 广西壮族自治区,南宁市,西乡塘区value: 2
			   */
		    Map<String, String> weekBackMap = weekBackDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Row row) throws Exception {
					String province = row.getString(0);
					String city = row.getString(1);
					String area = row.getString(2);
					String user_count = String.valueOf(row.getLong(3));
					return new Tuple2<String, String>(province + "," + city + "," + area, "C_" + user_count);
				}
			})
			.reduceByKeyLocally(new Function2<String, String, String>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {

					return v1+v2;
				}
			});
		   
		    //近期流失，连续2周没有启动过应用的用户(三周为一个周期，第一周登陆了，二三周没有登陆),正确的
		    Dataset<Row> recentLostDSet = spark.sql("select province f_province_id,"
		    		+ "city f_city_id,"
		    		+ "area f_area_id,"
		    		+ "count(z.userid) s_recent_lost "
		    		+ "FROM(select y.province,y.city,y.area,y.userid "
		    		+ "FROM (select x.province,x.city,x.area,x.userid,sum(WEEK) num1,SUM(week_cha) num2 "
		    		+ "FROM(SELECT province,city,area,userid,WEEK,WEEK-cn week_cha "
		    		+ "FROM (SELECT t.*, row_number() over(PARTITION BY userid ORDER BY WEEK ASC) cn "
		    		+ " FROM (SELECT province,city,area,userid,CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-1814400,'yyyy-MM-dd '))/7) AS INT) AS WEEK "
		    		+ " FROM t_logindata  "
		    		+ " WHERE logintime >= FROM_UNIXTIME(UNIX_TIMESTAMP()-1814400,'yyyy-MM-dd ') " 
		    		+ " GROUP BY CAST ((DATEDIFF(logintime,FROM_UNIXTIME(UNIX_TIMESTAMP()-1814400,'yyyy-MM-dd '))/7) AS INT),province,city,area,userid) t) s " 
		    		+ " GROUP BY province,city,area,userid,WEEK,WEEK-cn)x"
		    		+ " group by x.province,x.city,x.area,x.userid)y "
		    		+ " where num1=0 and num2=-1)z"
		    		+ " group by z.province,z.city,z.area");
			//recentLostDSet.write().mode("append").jdbc(Constants.JDBC_DBLELE, Constants.T_USER_RECENT_LOST, Constants.JdbcCon());
		    /**
			   * 近期流失Map<[省,市,区],user_count>   
			   * key: 河北省,廊坊市,文安县 value: 2
			   * key: 广西壮族自治区,南宁市,西乡塘区value: 2
			   */
		   Map<String, String> recentLostDMap = recentLostDSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Row row) throws Exception {
					String province = row.getString(0);
					String city = row.getString(1);
					String area = row.getString(2);
					String user_count = String.valueOf(row.getLong(3));
					return new Tuple2<String, String>(province + "," + city + "," + area, "D_" + user_count);
				}
			})
			.reduceByKeyLocally(new Function2<String, String, String>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {

					return v1+v2;
				}
			});
		    
			
		   //weekBackMap
		    //recentLostDMap
		     //loyalUserDMap
			  //continueActiveUserMap
		   
			   HashMap<String, String> resultMap1 = new HashMap<>();
			   HashMap<String, String> resultMap2 = new HashMap<>();
			   HashMap<String, String> resultMap3 = new HashMap<>();
			   HashMap<String, String> resultMap4 = new HashMap<>();
			   HashMap<String, String> resultMap5 = new HashMap<>();
			   HashMap<String, String> resultMap6 = new HashMap<>();
			   
			   							//遍历的Map
			   Set<String> pCAkeySet = weekBackMap.keySet();
			    for (String key : pCAkeySet) {
			    	String values1 = weekBackMap.get(key);
			    	String values2 = recentLostDMap.get(key);
			    	if(!recentLostDMap.containsKey(key)){  //若省市区不存在，把week插入新的<newkey,[0,0,0,count]>  √
			    		//recentLostValues加入到map3中，且value=   0，count1
			    		resultMap1.putAll(weekBackMap);
			    		resultMap1.putAll(recentLostDMap);
			    	}
			    	else{//若省市区相等，把<key,[0,0,42,count]>  √
			    		resultMap2.put(key, values1 + "," + values2);
			    	}
			    }
			    
			    resultMap1.putAll(resultMap2);
			    
			    Set<String> keySet = resultMap1.keySet();
			    for (String key : keySet) {
			    	String values1 = resultMap1.get(key);
			    	String values2 = loyalUserDMap.get(key);
			    	if(!loyalUserDMap.containsKey(key)){ //若省市区不存在，把上一个Map插入新的<newkey,[0,0,0,count]>
			    		//recentLostValues加入到map3中，且value=   0,0,count2
			    		resultMap3.putAll(resultMap1);
			    		resultMap3.putAll(loyalUserDMap);
			    	}
			    	else{//若省市区相等，把<key,[0,0,42,count]>  
			    		resultMap4.put(key, values1 + "," + values2);
			    	}
				}
			    
			    resultMap3.putAll(resultMap4);
			    
			    Set<String> keySet2 = resultMap3.keySet();
			    for (String key : keySet2) {
			    	String values1 = resultMap3.get(key);
			    	String  values2 = continueActiveUserMap.get(key);
			    	if(!continueActiveUserMap.containsKey(key)){ //若省市区不存在，把上一个Map插入新的<newkey,[0,0,0,count]>
			    		//recentLostValues加入到map3中，且value=  0,0,0,count3
			    		resultMap5.putAll(resultMap3);
			    		resultMap5.putAll(continueActiveUserMap);
			    	}
			    	else{ //若省市区相等，把<key,[0,0,42,count]>  √
			    		resultMap6.put(key, values1 + "," + values2);
			    	}
				}
			    
			    resultMap5.putAll(resultMap6);
			    
			    UserAactiveFrequency userAactiveFrequency = new UserAactiveFrequency();
			    
			    //对最终结合resultMap5做遍历
			    for(Entry<String, String> resultMap:resultMap5.entrySet()){
			    	
			    	String[] split2 = resultMap.getKey().split(",");
			    	String f_province_id = split2[0];   //省
			    	String f_city_id = split2[1];   //市
			    	String f_area_id = split2[2];   //区
			    	
			    	String value = resultMap.getValue();
			    	Long s_loyal_user = new Long(0L); //忠诚用户
			    	Long s_continue_active = new Long(0L); //连续活跃
			    	Long s_backflow = new Long(0L); //本周回流
			    	Long s_recent_lost = new Long(0L); //近期流失
			    	
			    	 if(value.contains("A_")){ //连续活跃
						 String substring = value.substring(value.indexOf("A_")+2, value.length());
						 if(substring.contains(",")){
							 Long sub = Long.valueOf(substring.split(",")[0]);
							 userAactiveFrequency.setS_continue_active(sub);
						 }
						 else{
							 userAactiveFrequency.setS_continue_active(Long.valueOf(substring));
						 }
						 s_continue_active = userAactiveFrequency.getS_continue_active();
					 } //连续活跃end
			    	 
			    	 if(value.contains("B_")){ //忠诚用户
						 String substring = value.substring(value.indexOf("B_")+2, value.length());
						 if(substring.contains(",")){
							 String sub = substring.split(",")[0];
							 userAactiveFrequency.setS_loyal_user(Long.valueOf(sub));
						 }
						 else{
							 userAactiveFrequency.setS_loyal_user(Long.valueOf(substring));
						 }
						 s_loyal_user = userAactiveFrequency.getS_loyal_user();
					 } //忠诚用户end

			    	 if(value.contains("C_")){ //本周回流
						 String substring = value.substring(value.indexOf("C_")+2, value.length());
						 if(substring.contains(",")){
							 String sub = substring.split(",")[0];
							 userAactiveFrequency.setS_backflow(Long.valueOf(sub));
						 }
						 else{
							 userAactiveFrequency.setS_backflow(Long.valueOf(substring));
						 }
						 s_backflow = userAactiveFrequency.getS_backflow();
					 } //本周回流end
			    	 
			    	 if(value.contains("D_")){ //近期流失
						 String substring = value.substring(value.indexOf("D_")+2, value.length());
						 if(substring.contains(",")){
							 String sub = substring.split(",")[0];
							 userAactiveFrequency.setS_recent_lost(Long.valueOf(sub));
						 }
						 else{
							 userAactiveFrequency.setS_recent_lost(Long.valueOf(substring));
						 }
					 s_recent_lost = userAactiveFrequency.getS_recent_lost();
					 } //近期流失end
			    	
			    	userAactiveFrequency.setF_province_id(f_province_id);  //省
			    	userAactiveFrequency.setF_city_id(f_city_id);  //市
			    	userAactiveFrequency.setF_area_id(f_area_id);   //区
			    	userAactiveFrequency.setS_loyal_user(s_loyal_user);  //忠诚用户
			    	userAactiveFrequency.setS_continue_active(s_continue_active); //连续活跃
			    	userAactiveFrequency.setS_backflow(s_backflow);  //本周回流
			    	userAactiveFrequency.setS_recent_lost(s_recent_lost);  //近期流失
			    	userAactiveFrequency.setS_report_date(DateUtils.getYesterdayDate());
					
					//执行插入方法
					userAactiveFrequencyDAO getuserAactiveFrequencyDAO = DAOFactory.getuserAactiveFrequencyDAO();
					getuserAactiveFrequencyDAO.insert(userAactiveFrequency); 
					//}
			    
			    
			    
			    spark.stop();
		    }//if rdd.isEmpty处理的结尾
	}//main的结尾

}