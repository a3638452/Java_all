package com.orange.service;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.bean.PageJumpRateBean;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.NumberUtils;
import com.orange.dao.PageJumpRateDAO;
import com.orange.dao.factory.DAOFactory;
/**
 * 页面跳出率
 * @author Administrator
 *
 */
public class PageJumpRate implements Serializable {

	private static final long serialVersionUID = 1L;

	public void  pageJumpRate(SparkSession sparkHDFS) {
		
		//从hdfs查找出页面数据并转成DataFrame并放到list集合中
		Dataset<Row> pageDataRDD = getPageDataCreateDFrame(sparkHDFS);
		//核心算法，并把结果入库
		getPagesCountAndInsertMysql(pageDataRDD);
	};


	/**
	 * 从hdfs查找出页面数据并转成DataFrame并放到list集合中
	 * @param sparkHDFS
	 * @return
	 */
	private static Dataset<Row> getPageDataCreateDFrame(SparkSession sparkHDFS) {

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
			    peopleDataFrame.createOrReplaceTempView("t_pagedata");
			    
			    //拿到page数据
			    Dataset<Row> DSet = sparkHDFS.sql("SELECT userid,pagename,logintime,logouttime "
			    		+ "FROM t_pagedata "
			    		+ "WHERE logouttime is not null and userid !='' and pagename is not null and pagename !='' "
			    		+ "ORDER BY userid,logintime").persist();
			    
				return DSet;
	}
	
	/**
	 * 核心算法,查找出跳出页面和访问页面，并做出页面跳出率插入mysql
	 * @param pageDataRDD
	 */
	private static void getPagesCountAndInsertMysql(Dataset<Row> pageDataRDD) {
		long lastTime = 0l;
	    long time = 0l;
	    String lastPage = "";
	    String nowPage = "";
	    Date date = null;
	    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    ArrayList<String> exitPagesList = new ArrayList<>();
	    ArrayList<String> allPagesList = new ArrayList<>();
		HashMap<String, Integer> exitPageMap = new HashMap<>();
		HashMap<String, Integer> allPageMap = new HashMap<>();
		
		List<Row> collectAsList = pageDataRDD.collectAsList();
	    for (Row row : collectAsList) {
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
	    
	    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
	    for (String pages : exitPagesList) {
	    	Integer count = exitPageMap.get(pages);  //统计list中各页面出现的次数
	    	exitPageMap.put(pages, (count == null ? 1 : count + 1));
		}
	    
	    //exitPageMap，存放的是《跳出页面,count(跳出页面)》
	    for (String pages : allPagesList) {
	    	Integer count = allPageMap.get(pages);  //统计list中各页面出现的次数
	    	allPageMap.put(pages, (count == null ? 1 : count + 1));
		}
	    
	    PageJumpRateBean pageJumpRateBean = new PageJumpRateBean();
	    //count(退出页(lastPage))/改页被访问的总次数
	    for (Entry<String, Integer> exitmap : exitPageMap.entrySet()) {
	    	String exitPageKey = exitmap.getKey();
	    	//若页面匹配，则getExitPagevalue/getAllPagevalue相除
	    	if(allPageMap.containsKey(exitPageKey)){
	    		Integer exitPageVol = exitmap.getValue();
	    		Integer visitPageVol = allPageMap.get(exitPageKey);
	    		Double jump_rates = NumberUtils.formatDouble(Double.valueOf(Long.valueOf(exitPageVol))/Double.valueOf(Long.valueOf(visitPageVol)),2);
	    		
	    		pageJumpRateBean.setPagename(exitPageKey);
	    		pageJumpRateBean.setJump_rate(String.valueOf(jump_rates));
	    		pageJumpRateBean.setReport_date(DateUtils.getYesterdayDate());
	    		
	    		PageJumpRateDAO pageJumpRateDAO = DAOFactory.getPageJumpRateDAO();
	    		pageJumpRateDAO.insert(pageJumpRateBean);
	    	}
	    	
	    	
		}
		
	}



}
