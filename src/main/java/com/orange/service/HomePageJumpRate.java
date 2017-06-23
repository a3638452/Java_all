package com.orange.service;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import com.orange.bean.HomeJumpRate;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.NumberUtils;
import com.orange.dao.HomeJumpRateDAO;
import com.orange.dao.factory.DAOFactory;

/**
 * e学产品跳出率
 * @author Administrator
 *
 */
public class HomePageJumpRate implements Serializable{

	private static final long serialVersionUID = 1L;

		public void homePageJumpRate(SparkSession sparkHDFS) {
		  
		//从hdfs查找出页面数据并转成DataFrame
		List<Row> collect = getPageDataCreateDFrame(sparkHDFS);
		//核心算法，计算首页访问量/日访问总量
		String homeJumpRate = getHomeJumpRateAndInsertMysql(collect);
		//计算Android首页访问量/日访问总量
		String androidJumpRate = getAndroidJumpRateAndInsertMysql(collect);
		//计算IOS首页访问量/日访问总量
		String iosJumpRate = getIOSJumpRateAndInsertMysql(collect);
		//入库
		getInsetMysql(homeJumpRate,androidJumpRate,iosJumpRate);
	};


		/**
		 * 从hdfs查找出页面数据并转成DataFrame
		 * @param sparkHDFS
		 * @return
		 */
		private static List<Row> getPageDataCreateDFrame(SparkSession sparkHDFS) {

			 JavaRDD<String> pageRDD = sparkHDFS.sparkContext().textFile(Constants.HDFS_PAGEDATA_YESTERDAY, 3).toJavaRDD();
			//JavaRDD<String> pageRDD = sparkHDFS.sparkContext().textFile("hdfs://master:9000/test/pagedata20170611.txt", 3).toJavaRDD();
			   String schemaString = "userid pagename logintime logouttime";

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
							
							if(attributes.length ==5 && attributes[0].length() <= 40){
								
								return RowFactory.create(attributes[0],attributes[1],attributes[3],attributes[4]);
							}else{
								return RowFactory.create(null,null,null,null);
							}  
						}
				});
				    // Apply the schema to the RDD
				    Dataset<Row> peopleDataFrame = sparkHDFS.createDataFrame(rowRDD, schema);
				 // Creates a temporary view using the DataFram
				    peopleDataFrame.createOrReplaceTempView("t_pagedata");
				    //拿到page数据
				    Dataset<Row> DSet = sparkHDFS.sql("SELECT userid,pagename,logintime,logouttime "
				    		+ "FROM t_pagedata "
				    		+ "WHERE logouttime is not null and userid !='' and pagename is not null and pagename !='' "
				    		+ "ORDER BY userid,logintime");
				    
				    DSet.createOrReplaceTempView("t_sort_data");
				    
				    List<Row> collect = DSet.persist().toJavaRDD().collect();
					return collect;			
		}
		
		/**
		 * 核心算法，计算首页访问量/日访问总量
		 * @param peopleDataFrame
		 */
		private static String getHomeJumpRateAndInsertMysql(List<Row> pageDataCollector) {
		    
			long lastTime = 0l;
		    long time = 0l;
		    String lastPage = "";
		    String nowPage = "";
		    Date date = null;
		    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    ArrayList<String> exitPagesList = new ArrayList<>();
		    ArrayList<String> allPagesList = new ArrayList<>();
		    for (Row row : pageDataCollector) {
		    	lastPage = nowPage;
		    	String pagename = row.getString(1);
		    	String logouttime = row.getString(3);
	    		lastTime = time;
	    		nowPage = pagename;
		    	try {
					 date = simpleDateFormat.parse(logouttime);
				} catch (ParseException e) {
					e.printStackTrace();
				}
		    	
		    	time = date.getTime();
		    	
		    	//存放所有页面的的list
		    	allPagesList.add(pagename);
		    	
		    	if((time-lastTime)>60000){  
		    		//上下两个的登出时间的差大于60秒的，视为退出页面，存放退出页面的list
		    		exitPagesList.add(lastPage);
		    	}
		    	
			}
		    
		    int homeExitPages = 0;
		    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
		    for (String pages : exitPagesList) {
		    	if(pages.equals("ServiceFragment")||pages.equals("IndexActivity")||pages.equals("MsgFragment")||
	    		   pages.equals("MySelfFragment")||pages.equals("JZServiceViewController")||pages.equals("JZMessageViewController")||
	    		   pages.equals("JZPersonalViewController"))
		    		{
		    		homeExitPages += 1;
		    		}
			}
		    
		    int homeAllPages = 0;
		    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
		    for (String pages : allPagesList) {
		    	if(pages.equals("ServiceFragment")||pages.equals("IndexActivity")||pages.equals("MsgFragment")||
	    		   pages.equals("MySelfFragment")||pages.equals("JZServiceViewController")||pages.equals("JZMessageViewController")||
	    		   pages.equals("JZPersonalViewController"))
		    		{
		    		homeAllPages += 1;
		    		}
				}
		    
		    System.out.println("homeExitPages:"+homeExitPages);
		    System.out.println("homeAllPages:"+homeAllPages);
		    Double jump_rates = NumberUtils.formatDouble(Double.valueOf(Long.valueOf(homeExitPages))/Double.valueOf(Long.valueOf(homeAllPages)),2);
			String jump_rate = jump_rates.toString();
			return jump_rate;
			
		}
		
		/**
		 * 核心算法，计算首页访问量/日访问总量
		 * @param peopleDataFrame
		 */
		private static String getAndroidJumpRateAndInsertMysql(List<Row> collect) {
			long lastTime = 0l;
		    long time = 0l;
		    String lastPage = "";
		    String nowPage = "";
		    Date date = null;
		    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    ArrayList<String> exitPagesList = new ArrayList<>();
		    ArrayList<String> allPagesList = new ArrayList<>();
		    for (Row row : collect) {
		    	lastPage = nowPage;
		    	String pagename = row.getString(1);
		    	String logouttime = row.getString(3);
	    		lastTime = time;
	    		nowPage = pagename;
		    	try {
					 date = simpleDateFormat.parse(logouttime);
				} catch (ParseException e) {
					e.printStackTrace();
				}
		    	
		    	time = date.getTime();
		    	
		    	//存放所有页面的的list
		    	allPagesList.add(pagename);
		    	
		    	if((time-lastTime)>60000){  
		    		//上下两个的登出时间的差大于60秒的，视为退出页面，存放退出页面的list
		    		exitPagesList.add(lastPage);
		    	}
		    	
			}
		    
		    int homeExitPages = 0;
		    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
		    for (String pages : exitPagesList) {
		    	if(pages.equals("ServiceFragment")||pages.equals("IndexActivity")||pages.equals("MsgFragment")||
	    		   pages.equals("MySelfFragment"))
		    		{
		    		homeExitPages += 1;
		    		}
			}
		    
		    int homeAllPages = 0;
		    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
		    for (String pages : allPagesList) {
		    	if(pages.equals("ServiceFragment")||pages.equals("IndexActivity")||pages.equals("MsgFragment")||
	    		   pages.equals("MySelfFragment"))
		    		{
		    		homeAllPages += 1;
		    		}
				}
		    
		    System.out.println("AndroidhomeExitPages:"+homeExitPages);
		    System.out.println("AndroidhomeAllPages:"+homeAllPages);
		    Double jump_rates = NumberUtils.formatDouble(Double.valueOf(Long.valueOf(homeExitPages))/Double.valueOf(Long.valueOf(homeAllPages)),2);
			String jump_rate = jump_rates.toString();
			return jump_rate;
		}
		
		/**
		 * 计算IOS首页访问量/日访问总量
		 * @param collect
		 */
			private static String getIOSJumpRateAndInsertMysql(List<Row> pageDataCollector) {

				long lastTime = 0l;
			    long time = 0l;
			    String lastPage = "";
			    String nowPage = "";
			    Date date = null;
			    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			    ArrayList<String> exitPagesList = new ArrayList<>();
			    ArrayList<String> allPagesList = new ArrayList<>();
			    for (Row row : pageDataCollector) {
			    	lastPage = nowPage;
			    	String pagename = row.getString(1);
			    	String logouttime = row.getString(3);
		    		lastTime = time;
		    		nowPage = pagename;
			    	try {
						 date = simpleDateFormat.parse(logouttime);
					} catch (ParseException e) {
						e.printStackTrace();
					}
			    	
			    	time = date.getTime();
			    	
			    	//存放所有页面的的list
			    	allPagesList.add(pagename);
			    	
			    	if((time-lastTime)>60000){  
			    		//上下两个的登出时间的差大于60秒的，视为退出页面，存放退出页面的list
			    		exitPagesList.add(lastPage);
			    	}
			    	
				}
			    
			    int homeExitPages = 0;
			    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
			    for (String pages : exitPagesList) {
			    	if(pages.equals("JZServiceViewController")||pages.equals("JZMessageViewController")||
		    		   pages.equals("JZPersonalViewController"))
			    		{
			    		homeExitPages += 1;
			    		}
				}
			    
			    int homeAllPages = 0;
			    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
			    for (String pages : allPagesList) {
			    	if(pages.equals("JZServiceViewController")||pages.equals("JZMessageViewController")||
		    		   pages.equals("JZPersonalViewController"))
			    		{
			    		homeAllPages += 1;
			    		}
					}
			    
			    System.out.println("IOShomeExitPages:"+homeExitPages);
			    System.out.println("IOShomeAllPages:"+homeAllPages);
			    Double jump_rates = NumberUtils.formatDouble(Double.valueOf(Long.valueOf(homeExitPages))/Double.valueOf(Long.valueOf(homeAllPages)),2);
				String jump_rate = jump_rates.toString();
				return jump_rate;
		}
			
			/**
			 * 入库
			 * @param homeJumpRate
			 * @param androidJumpRate
			 * @param iosJumpRate
			 */
			private static void getInsetMysql(String homeJumpRate, String androidJumpRate, String iosJumpRate) {
				 HomeJumpRate homeJumpRatebean = new HomeJumpRate();
				
				 homeJumpRatebean.setReport_date(DateUtils.getYesterdayDate());
				 homeJumpRatebean.setExue_jump_rate(homeJumpRate);
				 homeJumpRatebean.setAndroid_jump_rate(androidJumpRate);
				 homeJumpRatebean.setIos_jump_rate(iosJumpRate);
			    
			    HomeJumpRateDAO homeJumpRateDAO = DAOFactory.getHomeJumpRateDAO();
			    homeJumpRateDAO.insert(homeJumpRatebean);
				
			}


}
