package com.orange.service;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.bean.ArticleUserSets;
import com.orange.bean.UserTags;
import com.orange.bean.UsersetsUser;
import com.orange.dao.ArticleUserSetsDAO;
import com.orange.dao.UserTagsDAO;
import com.orange.dao.UsersetsUserDAO;
import com.orange.dao.factory.DAOFactory;


/**
 * 文章推荐
 * @author Administrator
 */
 public class ArticleRecommends implements Serializable{

	private static final long serialVersionUID = 1L;
	public void articleRecommends(SparkSession spark ,JavaRDD<ConsumerRecord<String, String>> lines, 
			Map<String, String> l5ArticlesMap, Dataset<Row> user_tagDSet, Dataset<Row> hadReadMapDSET, 
			Dataset<Row> hadRecommendDSet) throws InterruptedException{
		
		//1.数据流RDD转成List
		ArrayList<String> inputTransformtoList = inputTransformtoList(lines);
		
		//2.拿出用户的数据集合
		if(inputTransformtoList.size()>0){
			for (String str : inputTransformtoList) {
        		  String[] split = str.split(",");
	              if(split.length == 3){
		              String article_ids = split[0];
		              String label = split[1];
		              String user_ids = split[2];
			          Pattern pattern=Pattern.compile("/v1/getAdv*");
			          Matcher matcher=pattern.matcher(label); 
			          while(matcher.find()){    
			        	 // (article_id,  article_time +","+s_tag);
			        	
			        	  String user_tagss = new String();
			        	  try {
			        		   user_tagss = l5ArticlesMap.get(article_ids).split(",")[1];
						} catch (Exception e) {
						}
			        	  
			        	  if(user_tagss.length() > 0){
			        	  String[] splits = user_tagss.split(";");
			        	  for (int i = 0; i < splits.length; i++) {
			        		  String user_tag = splits[i];
			        		  List<Row> collectAsList = user_tagDSet
			        				  .select("user_id")
			        				  .filter("user_id = '"+user_ids+"' AND user_tag = '"+user_tag+"'")
			        				  .coalesce(30)
			        				  .collectAsList();
			        		  int id = (collectAsList.isEmpty() ? 0 : 1 );
			        		  //把用户画像插入画像表
			        		  getUserTagsInsert(user_ids,user_tag);
			        		  if(id == 0 ){
			        			  //3.查出15提前的文章推荐给用户
			        			  getCountMoreArticleInsert(user_ids,article_ids,user_tag,spark,l5ArticlesMap,hadReadMapDSET,hadRecommendDSet);
			        		  }else if(id == 1){ 
			        			  //4.把一天的文章资源推荐给该用户
			        			  getCountOneDayArticleInsert(user_ids,article_ids,user_tag,spark,user_tagDSet,l5ArticlesMap,hadReadMapDSET,hadRecommendDSet);
			        		  }
	                      }
		                }
			          }
	              }  
			}
		}
	}
	


	/**
	 * 数据流RDD转成List
	 * @param lines
	 * @return
	 */
	private ArrayList<String> inputTransformtoList(JavaRDD<ConsumerRecord<String, String>> lines) {
		 ArrayList<String> inputList = new ArrayList<String>();
			try {
				List<String> input = lines.map(new Function<ConsumerRecord<String,String>, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public String call(ConsumerRecord<String, String> row) throws Exception {
		                  String[] split = row.value().split("\",\"");//根据传入的数据按照"\"进行分割
		                  if(split.length == 16){ //过滤掉其他不符合要求的数据
			                  String user_id = split[7]; //获取用户id
			                  String label = split[4];	//获取标签
			                  String article_id = split[15].substring(0, split[15].length() - 1);  //获取文章id
			                  return article_id + "," +label + "," + user_id;
		                  }
		                  return "";
		              }
		          }).collect();   //这有问题，比较耗时。不管传入任何数据进来，此处以上都会计算解析一下，没有必要!
				inputList.addAll(input);
			} catch (Exception  e) {
	            e.printStackTrace();
			}
			return inputList;
	}
	
	/**
	 * 把用户画像插入画像表
	 * @param user_ids
	 * @param user_tag
	 */
	private void getUserTagsInsert(String user_ids, String user_tag) {

		ArrayList<UserTags> arrayList_tag = new ArrayList<>();
	    	UserTags userTags = new UserTags();
	        userTags.setUser_id(user_ids);
	        userTags.setUser_tag(user_tag);
	        userTags.setS_level("1");
	        arrayList_tag.add(userTags);
	        if(user_ids!=null && !user_ids.equals("")){
	        	UserTagsDAO userTagimpl = DAOFactory.getUserTags();
	        	userTagimpl.insertBatch(arrayList_tag);
	        	//AppendToLogFile.appendFile("\r\n"+DateUtils.getNowTime()+",画像计算完毕：" +user_ids+":"+user_tag, Constants.LOGURL);
	        }
	}
	
	
	/**
	 * 查出15提前的文章推荐给用户
	 * @param user_tag 
	 * @param article_ids 
	 * @param user_ids 
	 * @param spark
	 * @param allArticlesMap
	 * @param hadReadMapDSET 
	 * @param hadRecommendDSet 
	 */
	private void getCountMoreArticleInsert(String user_ids, String article_ids, String user_tag, SparkSession spark, 
			Map<String, String> l5ArticlesMap, Dataset<Row> hadReadMapDSET, Dataset<Row> hadRecommendDSet) {
		//article_id,  article_time +","+s_tag
		HashMap<String, String> recommendMap = new HashMap<>();
		for (Entry<String, String> map : l5ArticlesMap.entrySet()) {   //key:id+time,value:tag
			String tag = map.getValue().split(",")[1];
	   		if(tag.indexOf(user_tag)>=0){
	   			 String article_id = map.getKey();
	   			 String article_time = map.getValue().split(",")[0];
	   			 recommendMap.put(article_id, article_time);
	   		}
		}
    
		List<Row> collectAsList = hadReadMapDSET.select("s_article_id").filter("s_user_id  = '"+user_ids+"'").coalesce(30)
		.collectAsList();
       	ArrayList<String> HadReadList = new ArrayList<>();
       	for (Row row : collectAsList) {
       		HadReadList.add(row.getString(0));
		}
	 	//!!!!jdbc查出已读，做成List
       	//AppendToLogFile.appendFile("\r\n"+"2.已读文章集合..." ,Constants.LOGURL );
//       	List<Row> collectAsList = spark.sql("select s_article_id "
//       			+ "from t_plat_user_article_map where s_user_id  = '"+user_ids+"' "  )
//       			.collectAsList();
//       	ArrayList<String> HadReadList = new ArrayList<>();
//       	for (Row row : collectAsList) {
//       		HadReadList.add(row.getString(0));
//		}
       	
       	List<Row> collectAsList2 = hadRecommendDSet
       	.select("article_id")
       	.filter("create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
       	.coalesce(30).collectAsList();
       	
		 ArrayList<String> hadRecommendList = new ArrayList<>();
		 for (Row row : collectAsList2) {
			 hadRecommendList.add(row.getString(0));
		}
		

		HashMap<String, String> map1 = new HashMap<>();
		//AppendToLogFile.appendFile("\r\n"+"去除已读文章操作..." ,Constants.LOGURL );
		for (Entry<String, String> map : recommendMap.entrySet()) {
			String article_id = map.getKey();
			if(!HadReadList.contains(article_id)){
				map1.put(article_id, map.getValue());
			}
		} //去除已

		ArrayList<ArticleUserSets> arrayList1_1 = new ArrayList<>();
		ArrayList<UsersetsUser> arrayList1_2 = new ArrayList<>();
		ArrayList<UsersetsUser> arrayList1_3 = new ArrayList<>();

		//AppendToLogFile.appendFile("\r\n"+"将文章推荐列表和用户集合表插入mysql..." ,Constants.LOGURL );
		Set<String> keySet = map1.keySet();
		for (String key : keySet) {
			ArticleUserSets articleUserSets = new ArticleUserSets();   //推荐列表用户集合的bean
			UsersetsUser usersetsUser = new UsersetsUser();            //推荐列表文章的bean
			UsersetsUser usersetsUser2 = new UsersetsUser();            //推荐列表文章的bean
			String article_time = map1.get(key);
			if (!hadRecommendList.contains(key)) {
				articleUserSets.setArticle_id(key);
				articleUserSets.setUserset_id(key);
				articleUserSets.setRecommend_type("1");
				articleUserSets.setOpt_type("0");
				articleUserSets.setArticle_time(article_time);
				arrayList1_1.add(articleUserSets);
      
				usersetsUser.setUser_id(user_ids); 
				usersetsUser.setUserset_id(key);
				arrayList1_2.add(usersetsUser);
			}else{
				usersetsUser2.setUser_id(user_ids); 
				usersetsUser2.setUserset_id(key);
				arrayList1_3.add(usersetsUser2);
				//AppendToLogFile.appendFile("\r\n"+"1.for循环里面单插结束标志" ,Constants.LOGURL );
			} 
		}
		ArticleUserSetsDAO tUserSets = DAOFactory.getTUserSets();   
		tUserSets.insertBatch(arrayList1_1);
		UsersetsUserDAO tUser1 = DAOFactory.getTUser(); 
		tUser1.insertBatch(arrayList1_2);

		UsersetsUserDAO tUser2 = DAOFactory.getTUser();  
		tUser2.insertBatch(arrayList1_3);
	}
	
	
	/**
	 * 把一天的文章资源推荐给该用户
	 * @param user_ids
	 * @param article_ids
	 * @param user_tag
	 * @param spark
	 * @param user_tagDSet 
	 * @param allArticlesMap
	 * @param hadReadMapDSET 
	 * @param hadRecommendDSet 
	 */
	private void getCountOneDayArticleInsert(String user_ids, String article_ids, String user_tag, SparkSession spark,
			Dataset<Row> user_tagDSet, Map<String, String> l5ArticlesMap, Dataset<Row> hadReadMapDSET, Dataset<Row> hadRecommendDSet) {
		//查询出用户上一次的该文章类型阅读时间
		Timestamp timestamps = user_tagDSet.select("update_time")
		.filter("user_id='"+user_ids+"' AND user_tag='"+user_tag+"'")
		.sort(user_tagDSet.col("update_time").desc()).takeAsList(1).get(0).getTimestamp(0);
		
		String date = timestamps.toString();
       	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
       	Date lastCreateTime = null;
       	try {
       		lastCreateTime = simpleDateFormat.parse(date);
       	} catch (ParseException e) {
       		e.printStackTrace();
       	}
       	
       	//article_id,  article_time +","+s_tag);
       	HashMap<String, String> recommendMap = new HashMap<>();
       	Date article_date = null;
       	for (Entry<String, String> map : l5ArticlesMap.entrySet()) {   //  id+time,tag
       		String tag = map.getValue().split(",")[1];
       		String article_id = map.getKey();
       		String article_time = map.getValue().split(",")[0];
       		try {
       			article_date = simpleDateFormat.parse(article_time);
       		} catch (ParseException e) {
          		e.printStackTrace();
       		}
       		if(tag.indexOf(user_tag)>=0 && article_date.getTime() > lastCreateTime.getTime()){
       		   recommendMap.put(article_id, article_time);
       		}
       	}

       	
       	List<Row> collectAsList = hadReadMapDSET.select("s_article_id").filter("s_user_id  = '"+user_ids+"'").coalesce(30)
       			.collectAsList();
       	
       	ArrayList<String> HadReadList = new ArrayList<>();
       	for (Row row : collectAsList) {
       		HadReadList.add(row.getString(0));
		}
       	//!!!!jdbc查出已读，做成List
       	//AppendToLogFile.appendFile("\r\n"+"2.已读文章集合..." ,Constants.LOGURL );
//       	List<Row> collectAsList = spark.sql("select s_article_id "
//       			+ "from t_plat_user_article_map where s_user_id  = '"+user_ids+"' "  )
//       			.collectAsList();
//       	ArrayList<String> HadReadList = new ArrayList<>();
//       	for (Row row : collectAsList) {
//       		HadReadList.add(row.getString(0));
//		}
       	// AppendToLogFile.appendFile("\r\n"+"2.查出已推荐..." ,Constants.LOGURL );
       	
     	List<Row> collectAsList2 = hadRecommendDSet
     	       	.select("article_id")
     	       	.filter("create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
     	       	.coalesce(30).collectAsList();
     	
     	 ArrayList<String> hadRecommendList = new ArrayList<>();
		 for (Row row : collectAsList2) {
			 hadRecommendList.add(row.getString(0));
		}
           
       	HashMap<String, String> map1 = new HashMap<>();
       	//AppendToLogFile.appendFile("\r\n"+"2.去除已读..." ,Constants.LOGURL );
       	for (Entry<String, String> map : recommendMap.entrySet()) {
       		String article_id = map.getKey();
       		if(!HadReadList.contains(article_id)){
       			map1.put(article_id, map.getValue());
       		}
       	}  //去除已读
               
       	ArrayList<ArticleUserSets> arrayList2_1 = new ArrayList<>();
       	ArrayList<UsersetsUser> arrayList2_2 = new ArrayList<>();
        ArrayList<UsersetsUser> arrayList2_3 = new ArrayList<>();
               
        // AppendToLogFile.appendFile("\r\n"+"2.即将插入mysql..." ,Constants.LOGURL );
        Set<String> keySet = map1.keySet();
        for (String key : keySet) {
        	ArticleUserSets articleUserSets = new ArticleUserSets();   
            UsersetsUser usersetsUser = new UsersetsUser();  
            UsersetsUser usersetsUser2 = new UsersetsUser();  
            if (!hadRecommendList.contains(key)) {
            	String article_time = map1.get(key);
                articleUserSets.setArticle_id(key);
                articleUserSets.setUserset_id(key);
                articleUserSets.setRecommend_type("1");
                articleUserSets.setOpt_type("0");
                articleUserSets.setArticle_time(article_time);
                arrayList2_1.add(articleUserSets);

                usersetsUser.setUser_id(user_ids); 
                usersetsUser.setUserset_id(key);
                arrayList2_2.add(usersetsUser);
                //AppendToLogFile.appendFile("\r\n"+"2.for循环里双插结束标志..." ,Constants.LOGURL);
            } else{
            	usersetsUser2.setUser_id(user_ids); 
                usersetsUser2.setUserset_id(key);
                arrayList2_3.add(usersetsUser2);
                //AppendToLogFile.appendFile("\r\n"+"2.for循环里单插结束标志..." ,Constants.LOGURL);
        }
        ArticleUserSetsDAO tUserSets02 = DAOFactory.getTUserSets();
        tUserSets02.insertBatch(arrayList2_1);
        UsersetsUserDAO tUser02 = DAOFactory.getTUser();
        tUser02.insertBatch(arrayList2_2);
        // AppendToLogFile.appendFile("\r\n"+"2.外层双插结束标志..." ,Constants.LOGURL);
       
        UsersetsUserDAO tUser02_2 = DAOFactory.getTUser();
        tUser02_2.insertBatch(arrayList2_3);
        // AppendToLogFile.appendFile("\r\n"+"2.外层单插结束标志..." ,Constants.LOGURL);
        }
	}
}
