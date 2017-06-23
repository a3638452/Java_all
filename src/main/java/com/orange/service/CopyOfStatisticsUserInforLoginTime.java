package com.orange.service;


import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.bean.UserLifeCycle;
import com.orange.common.util.Constants;
import com.orange.common.util.SparkSessionHive;
import com.orange.dao.UserLifeCycleDAO;
import com.orange.dao.factory.DAOFactory;

import scala.Tuple2;

/**
 * 统计用户详细信息、登陆次数和一天中的访问时长
 * @author Administrator
 */
public class CopyOfStatisticsUserInforLoginTime   {
	

	 public static void main(String[] args) {

	  //1.构建sparksession
	  SparkSession spark = new SparkSessionHive().getSparkSession();
	  
	  	spark.sql("use sdkdata");
	    spark.sql("select t.userid,"
	    		+ "count(t.userid) login_count,"
	    		+ "sum(if (unix_timestamp(logouttime)-unix_timestamp(logintime) < 1800 ,(unix_timestamp(logouttime)-unix_timestamp(logintime)),0 )) as use_time"
	    		+ " FROM (select userid,logintime,logouttime "
	    		+ " FROM systimedata "
	    		+ " WHERE logintime >= '2017-01-16' and logintime < '2017-05-30' "
	    		+ " AND logouttime >= '2017-01-16' and logouttime < '2017-05-30') t"
	    		+ " GROUP by t.userid ")
	    		.createOrReplaceTempView("t_user_count_time");
	    //查询出用户信息表't_user_base'
	    spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
	    		.createOrReplaceTempView("t_user_base");
    
	    Dataset<Row> userLoginUserTimeRDD = spark.sql("SELECT a.userid,"
	    		+ "a.login_count,"
	    		+ "a.use_time,"
	    		+ "b.s_xiaoxincode,"
	    		+ "b.s_account,"
	    		+ "b.s_register_time,"
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_create_time, "
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date, "
	    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_update_time "
	    		+ " FROM t_user_count_time a,t_user_base b"
	    		+ " WHERE a.userid=b.p_id ");
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
		
	    UserLifeCycle userLifeCycle = new UserLifeCycle();
		   
		 for(Entry<String, String> kv:userLoginUserTimeMap.entrySet()){
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
		
	  spark.stop();
	  
	    }//if的外层
}

