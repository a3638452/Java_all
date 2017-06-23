package com.orange.common.util;

import java.util.Properties;



/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * Spark作业相关的常量
	 */
	String SPARK_LOCAL = "spark.local";
	String SPARK_PAGE_RATE = "PageConvertRate";
	String SPARK_SQL_DIR= "spark.sql.warehouse.dir";
	String WAREHOURSE_DIR = "file:${system:user.dir}/spark-warehouse";	
	String WAREOURSELOCATION = "/code/VersionTest/spark-warehouse";
	String URL_EXIAOXIN = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	//报表指标需要用到的hdfs和jdbc连接及表明
	//String HDFS_PAGEDATA_YESTERDAY = "hdfs://master:9000/SDKData/total/data"+DateUtils.getYesterdayDate()+"/pagedata"+DateUtils.getYesterdayDate()+".txt";
	String SPARK_REPORT = "报表指标APP";
	String T_USER_BASE = "t_user_base";
	String t_plat_send_history = "t_plat_send_history";
	String t_user_tags = "t_user_tags";
	String t_plat_user_article_map = "t_plat_user_article_map";
	String t_user_recommend = "t_user_recommend";
	
	String HDFS_URL_ANDROID_1D = "hdfs://master:9000/SDKData/android/data_android_" + DateUtils.getYesterdayDateyyMM() + "/pagedata_android" + DateUtils.getYesterdayDateyyMM() + ".txt";
	String HDFS_URL_ANDROID_7D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata7day.txt";
	String HDFS_URL_ANDROID_14D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata14day.txt";
	String HDFS_URL_ANDROID_30D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata30day.txt";
	String HDFS_URL_IOS_1D = "hdfs://master:9000/SDKData/ios/data_ios_" + DateUtils.getYesterdayDateyyMM() + "/pagedata_ios" + DateUtils.getYesterdayDateyyMM() +".txt";
	String HDFS_URL_IOS_7D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata7day.txt";
	String HDFS_URL_IOS_14D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata14day.txt";
	String HDFS_URL_IOS_30D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata30day.txt";
	
	/**
	 * 项目配置相关的常量
	 */
	//String PARQUET_PATH = "hdfs://master:9000/recommend/2017-03-21_recommend.parquet";
	String PARQUET_PATH = "hdfs://master:9000/recommend/"+DateUtils.getTodayDate()+"_recommend.parquet";
	String URL_TEST = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	String URL_TEST_EXIAOXIN = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	
	
	String JDBC_LIVING_STREAMING = "live_streaming";
	String JDBC_LIVING_STREAMING_DURATION = "live_streaming_duration";
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	
	String JDBC_URL2 = "url";
	String JDBC_USER2 = "user";
	String JDBC_PASSWORD2 = "password";
	//___________________________________________________________________________
	String JDBC_DRIVERS = "jdbc.driver";
	String JDBC_URLS = "jdbc.url";
	String JDBC_PASSWORDS = "jdbc.password";
	String JDBC_USEUNICODE="jdbc.useUnicode";
	String JDBC_CHARACTERENCODING="jdbc.characterEncoding";
	String SPARK_LOCALS = "spark.local";
	
	//mysql配置
	String JDBC_TABLE_SYS_APPVERSION = "report_app_version";//
	String JDBC_TABLE_NETWORK = "report_network_type";
	String JDBC_TABLE_SYSOSVERSION = "report_os_version";//
	String JDBC_TABLE_MODULE_COUNT = "report_modules";//
	
	String T_PLAT_SEND_HISTORY = "t_plat_send_history";
    String T_PLAT_USER_ARTICLE_MAP = "t_plat_user_article_map";
    String T_USER_TAGS = "t_user_tags";
    String T_USER_RECOMMEND = "t_user_recommend";
    String T_DISC_TOPIC = "t_disc_topic";
    String T_DISC_GROUP = "t_disc_group";
    String T_DISC_GROUP_USER_MAP = "t_disc_group_user_map";
    String T_USER_TOPIC_TAGS = "t_user_topic_tags";
    String T_USER_TOPIC_RECOMMEND = "t_user_topic_recommend";
	String T_USRE_LIFE_CYCLE = "t_report_user_login_usetime";
	String T_USER_ACTIVE_FREQUENCY = "t_report_user_active_frequency";
	String T_USER_CONTINUE_ACTIVE = "t_user_continue_active";
	String T_USER_LOYAL_USER = "t_user_loyal_user";
	String T_USER_BACKFLOW = "t_user_backflow";
	String T_USER_RECENT_LOST = "t_user_recent_lost";
	String T_PLAT_QUE = "t_plat_que";
	String T_PLAT_ANSWER = "t_plat_answer";
	String T_QUE_ANSER = "t_report_que_anser";
	String T_USER_REPORT = "t_user_report";
	String T_CLASS_TEACHER_MAP = "t_class_teacher_map";
	String T_STUDENT_PARENT_MAP = "t_student_parent_map";
	String T_STUDENT_CLASS_MAP = "t_student_class_map";
	String T_SCHOOL = "t_school";
	String T_SYS_DICT = "t_sys_dict";
	String T_REPORT_AREA_MODULE = "t_report_area_module";
	String LIVE_STREAMING = "live_streaming";
	String TEST_PAGE_USE_TIME = "test_page_use_time";
	String T_USER_MODULE_WEEK = "t_user_module_week";
	String T_USER_LOGIN_WEEK = "t_user_login_week";
	String T_MIDDLE_USETIME = "t_middle_usetime";
	
  /**
   * 报表相关配置
   */
	
	//报表指标需要用到的hdfs和jdbc连接及表明
	//String JDBC_EXIAOXIN = "jdbc:mysql://122.193.22.133:3310/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	String JDBC_EXIAOXIN = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	String JDBC_TEST_EXIAOXIN = "jdbc:mysql://192.168.0.18:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	String JDBC_DBLELE = "jdbc:mysql://122.193.22.133:3310/lele?useUnicode=true&characterEncoding=UTF-8";
	//String HDFS_PAGEDATA_YESTERDAY = "hdfs://master:9000/SDKData/total/data"+DateUtils.getYesterdayDate()+"/pagedata"+DateUtils.getYesterdayDate()+".txt";
	String HDFS_PAGEDATA_YESTERDAY = "hdfs://master:9000/SDKData/total/data"+DateUtils.getYesterdayDateyyMM()+"/pagedata"+DateUtils.getYesterdayDateyyMM()+".txt";
	//String HDFS_PAGEDATA_YESTERDAY = "hdfs://master:9000/test/pagetest.txt";
	//String HDFS_SYSTIMEDATA_YESTERDAY = "hdfs://master:9000/test/systimedata20170330xianshang.txt";
	String HDFS_SYSTIMEDATA_YESTERDAY = "hdfs://master:9000/SDKData/total/data"+DateUtils.getYesterdayDateyyMM()+"/systimedata"+DateUtils.getYesterdayDateyyMM()+".txt";
	//String HDFS_LOGINDATA_YESTERDAY = "hdfs://master:9000/test/logindata20170404xianshang.txt";
	String HDFS_LOGINDATA_YESTERDAY = "hdfs://master:9000/test/logindata20170405.txt";
	String HDFS_TEST = "hdfs://master:9000/test/logindataTestzhongcheng.txt";
	String T_USER_USE_TIME = "t_user_use_time";
	String T_USER_QUE_NUM = "t_user_que_num";
	String T_ANS_ACCEPT = "t_ans_accept";
	String T_REPORT_USER_STAYTIME = "t_report_user_staytime_copy";
	//本地的mysql配置
	/**
	 * Spark作业相关的常量
	 */
	String SPARK_APP_NAME_PAGE = "PageConvertRate";
	String SPARK_APP_VERSION_NAME = "indexstatics";
	
	
  public static Properties JdbcCon(){
		 Properties prop =  new Properties();
		 prop.put("user", "xxv2");
		 prop.put("password", "xv2PassWD-321");
		return prop;
	 }
  
  public static Properties testJdbcCon(){
		 Properties prop =  new Properties();
		 prop.put("user", "root");
		 prop.put("password", "test3pass");
		return prop;
	 }
  public static Properties JdbcConTest(){
		 Properties prop =  new Properties();
		 prop.put("user", "root");
		 prop.put("password", "test3pass");
		return prop;
	 }
}
