package com.orange.service;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.sparkproject.constant.Constants;
import com.orange.sparkproject.dao.ArticleUserSetsDAO;
import com.orange.sparkproject.dao.UserTagsDAO;
import com.orange.sparkproject.dao.UsersetsUserDAO;
import com.orange.sparkproject.dao.factory.DAOFactory;
import com.orange.sparkproject.domain.ArticleUserSets;
import com.orange.sparkproject.domain.UserTags;
import com.orange.sparkproject.domain.UsersetsUser;
import com.orange.sparkproject.util.AppendToLogFile;
import com.orange.sparkproject.util.DateUtils;

import scala.Tuple2;

/**
 * 文章推荐
 * @author Administrator
 */
 public class ArticleRecommends implements Serializable{
	

	private static final long serialVersionUID = 1L;
	public void articleRecommends(SparkSession spark ,JavaRDD<ConsumerRecord<String, String>> lines) throws InterruptedException{

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
	          }).collect(); 
			inputList.addAll(input);
		} catch (Exception  e) {
            e.printStackTrace();
		}
		
		//遍历关键字段
		if(inputList.size()>0){
			//多标签处理
			for (String str : inputList) {
        		  String[] split = str.split(",");
	              if(split.length == 3){
		              String article_ids = split[0];
		              String label = split[1];
		              String user_ids = split[2];
		             //AppendToLogFile.appendFile("\r\n"+DateUtils.getNowTime()+"获取到article_id:" +article_ids+ " label:" +label+" user_ids:" +user_ids, Constants.LOGURL);
		              //正则匹配，精准处理相应数据
			          Pattern pattern=Pattern.compile("/v1/getAdv*");
			          Matcher matcher=pattern.matcher(label); //用户数据的标志，判断是否计算
			          while(matcher.find()){//如果匹配到标志，才执行处理

			        	 //查询数据库获取<user_id,s_tag>
			        	 Dataset<Row> historyDF = spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_PLAT_SEND_HISTORY, Constants.JdbcConTest());
		                historyDF.createOrReplaceTempView("t_plat_send_history");   // 文章和标签匹配表t_plat_send_history

		                //查出用户阅读文章的标签
	                	List<Row> collect = spark.sql("select s_tag user_tag  from t_plat_send_history where p_id =  '"+article_ids+"'").toJavaRDD().collect();
	                	String user_tagss = new String();
	                	for (Row row : collect) {
	                		 user_tagss = row.getString(0);
	                	}
               //把标签取出来放到字符串中，以后要对多标签进行遍历处理
               //拆分标签，分条插入 
               ArrayList<UserTags> arrayList_tag = new ArrayList<>();
               String[] splits = user_tagss.split(";");
	               for (int i = 0; i < splits.length; i++) {
	            	   UserTags userTags = new UserTags();
		                String user_tag = splits[i];
		                userTags.setUser_id(user_ids);
		                userTags.setUser_tag(user_tag);
		                userTags.setS_level("1");
		                arrayList_tag.add(userTags);
		                
		                if(user_ids!=null && !user_ids.equals("")){
	                     UserTagsDAO userTagimpl = DAOFactory.getUserTags();
	                     userTagimpl.insertBatch(arrayList_tag);
	                     //AppendToLogFile.appendFile("\r\n"+DateUtils.getNowTime()+",画像计算完毕：" +user_ids+":"+user_tag, Constants.LOGURL);
		                }
	                    //拿到用户基本信息表
	                    spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TAGS, Constants.JdbcConTest())
	                    .createOrReplaceTempView("t_user_tags");
	                    //查出用户画像表中是否有这个类型的文章
	                    List<Row> timeSignList = spark.sql(" select count(1) from t_user_tags "
	                    		+ "where user_id = '"+user_ids+"' and user_tag = '"+user_tag+"' ")
	                    .toJavaRDD().collect();    //查出画像库中是否含有该用户的标签
	                    Long num = timeSignList.get(0).getLong(0);   

	                    if(num == 1 ){      //if = 1,画像表中不存在
//二.1___________________________________________计算15天的文章，并过滤两次已读文章____________________________________________________________________________________________________________________________________________
                    	//优化点：分隔符拆解可进行mysql函数的使用（，）
                        spark.sql("select p_id , s_creater_time  "
                                +"from t_plat_send_history where s_tag LIKE '%"+user_tag+"%' ")
                                .createOrReplaceTempView("t_tags_temp");//查出用户所属的所有标签的推荐文章
                        ////!!文章推荐集合 ,查出15天内的用户所属的所有标签
                        //AppendToLogFile.appendFile("\r\n"+"新用户，推荐给30天的近需文章..." ,Constants.LOGURL );
                         Map<String, String> recommendList = spark.sql("select p_id,s_creater_time "
                         		+ "from t_tags_temp  "
                         		+ "where s_creater_time  > FROM_UNIXTIME(UNIX_TIMESTAMP()-4320000,'yyyy-MM-dd 00:00:00') ")
                        		 
                                .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

								private static final long serialVersionUID = 1L;
								@Override
		                        public Tuple2<String, String> call(Row row) throws Exception {
									String article_id = String.valueOf(row.getLong(0));
		                            String article_time = String.valueOf(row.getTimestamp(1));
		                            return new Tuple2<String, String>(article_id, article_time);
		                        }
                         }).collectAsMap();
                         
                    //!!jdbc查询出用户已读文章，做成List
                   spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_PLAT_USER_ARTICLE_MAP, Constants.JdbcConTest())
                   .createOrReplaceTempView("t_plat_user_article_map");
                  //!!jdbc查出的已读文章，做成map
                  // AppendToLogFile.appendFile("\r\n"+"查出已读文章集合..." ,Constants.LOGURL );
                   Map<String, String> mapHadReadMap = spark.sql("select s_article_id , 0 as num "
                   		+ "from t_plat_user_article_map "
                   		+ "where s_user_id  = '"+user_ids+"' "  )
                 		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

								private static final long serialVersionUID = 1L;
								
								@Override
								public Tuple2<String, String> call(Row row) throws Exception {
		                             String article_id = String.valueOf(row.getLong(0));
		                             String article_time = String.valueOf(row.getInt(1));
		                             return new Tuple2<String, String>(article_id, article_time);
		                         }
                     }).collectAsMap();
                   
                   //!!hdfs查出来的昨天已推荐
                /** spark.read().parquet(Constants.PARQUET_PATH).createOrReplaceTempView("t_hdfs");
                 Map<String, String> hdfsMap = spark.sql("select article_id,recommend_type from t_hdfs ")
                         .javaRDD().mapToPair(new PairFunction<Row, String, String>() {
                         	
								private static final long serialVersionUID = 1L;

									@Override
                             public Tuple2<String, String> call(Row row)
                                     throws Exception {
                                 String article_id = row.getString(0);
                                 String article_time = row.getString(1);
                                 return new Tuple2<String, String>(article_id, article_time);
                             }
                         }).collectAsMap();**/

                  //!!!!DB查出的已推荐
                  spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_RECOMMEND, Constants.JdbcConTest())
                  .createOrReplaceTempView("t_user_recommend");
                 // AppendToLogFile.appendFile("\r\n"+"查出已推荐文章集合..." ,Constants.LOGURL );
                  //HashMap<String, String> dbRecommendMap = new HashMap<>();
                  //try {
                	  Map<String, String> dbRecommendMap = spark.sql("select article_id ,recommend_type from t_user_recommend "
                        		+ "where create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
                       		 .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

      							private static final long serialVersionUID = 1L;

      						@Override
                            public Tuple2<String, String> call(Row row)
                                    throws Exception {
                                String article_id = row.getString(0);
                                String article_time = String.valueOf(row.getInt(1));
                                return new Tuple2<String, String>(article_id, article_time);
                            }
                        }).collectAsMap();
//                	  Thread.sleep(1*1000);
//                	  dbRecommendMap.putAll(dbRecommendMaps);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//
//				}
                  
                  
                  HashMap<String, String> hadRecommendMap = new HashMap<String,String>();
                  //hadRecommendMap.putAll(hdfsMap);
                  hadRecommendMap.putAll(dbRecommendMap);
                  
                  
                   HashMap<String, String> map1 = new HashMap<>();
                   //AppendToLogFile.appendFile("\r\n"+"去除已读文章操作..." ,Constants.LOGURL );
                   for (Entry<String, String> map : recommendList.entrySet()) {
                       String article_id = map.getKey();
                       if(!mapHadReadMap.containsKey(article_id)){
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
                      String article_time = map1.get(key);
                      if (!hadRecommendMap.containsKey(key)) {
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
                          usersetsUser.setUser_id(user_ids); 
                          usersetsUser.setUserset_id(key);
                          arrayList1_3.add(usersetsUser);
                          //AppendToLogFile.appendFile("\r\n"+"1.for循环里面单插结束标志" ,Constants.LOGURL );
                          
                      } 
                 }
                  ArticleUserSetsDAO tUserSets = DAOFactory.getTUserSets();   
                  tUserSets.insertBatch(arrayList1_1);
                  UsersetsUserDAO tUser1 = DAOFactory.getTUser(); 
                  tUser1.insertBatch(arrayList1_2);
                  //AppendToLogFile.appendFile("\r\n"+"1.外层双插结束标志" ,Constants.LOGURL );
                  
                  UsersetsUserDAO tUser2 = DAOFactory.getTUser();  
                  tUser2.insertBatch(arrayList1_3);
                  //AppendToLogFile.appendFile("\r\n"+"1.外层单插结束标志" ,Constants.LOGURL );  
                  
                  
                  
                  
                  
              }//if结尾
                else if (num > 1){  //在画像中存在此tag，计算上一次画像时间到此刻的新文章，并入库前进行一次判断
//二.2_________________________________________________________________________________________________________________________________________________________________________________________________             
             	   //查询出用户上一次的该文章类型阅读时间
                   Timestamp lastCreateTime = spark.sql("SELECT MAX(update_time) FROM t_user_tags WHERE "
                            + "update_time < (SELECT MAX(update_time) FROM t_user_tags) and user_id = '"+user_ids+"' and user_tag = '"+user_tag+"' ")
                            .toJavaRDD().collect().get(0).getTimestamp(0);
                   //!!!!!!!!!查询出该时间段的所有文章资源,变成rdd
                   spark.sql("select p_id , s_creater_time  from t_plat_send_history "
                            + "where s_tag LIKE '%"+user_tag+"%' ")
                   .createOrReplaceTempView("t_all_tag_tmpl");
                  // AppendToLogFile.appendFile("\r\n"+"已读用户，查出上一次到最新的文章推荐集合..." ,Constants.LOGURL );
                    Map<String, String> recommendList = spark.sql("select p_id article_id, s_creater_time "
                    		+ "from t_all_tag_tmpl "                         
                            + " where s_creater_time >  '" + lastCreateTime + "' ")
                                .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<String, String> call(Row row)
                                     throws Exception {

									return new Tuple2<String, String>(String.valueOf(row.getLong(0)), String.valueOf(row.getTimestamp(1)));
								}
                     }).collectAsMap();

                    Dataset<Row> rddDSet = spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_PLAT_USER_ARTICLE_MAP, Constants.JdbcConTest());
                        rddDSet.createOrReplaceTempView("t_plat_user_article_map");  
                    //!!!!jdbc查出已读，做成Map
                   //AppendToLogFile.appendFile("\r\n"+"2.已读文章集合..." ,Constants.LOGURL );
                   Map<String, String> mapHadReadMap = spark.sql("select s_article_id , 0 as num "
                   		+ "from t_plat_user_article_map where s_user_id  = '"+user_ids+"' "  )
                 		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

             			  private static final long serialVersionUID = 1L;

             			  @Override
             			  public Tuple2<String, String> call(Row row)
                                 throws Exception {
                             String article_id = String.valueOf(row.getLong(0));
                             String article_time = String.valueOf(row.getInt(1));
                             return new Tuple2<String, String>(article_id, article_time);
                         }
                     }).collectAsMap();

                   //!!!!DB查出的已推荐，做成Map
                   spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_RECOMMEND, Constants.JdbcConTest())
                   .createOrReplaceTempView("t_user_recommend");
                  // AppendToLogFile.appendFile("\r\n"+"2.查出已推荐..." ,Constants.LOGURL );
                   Map<String, String> dbRecommendMap = spark.sql("select article_id ,recommend_type "
                   		+ "from t_user_recommend "
                   		+ "where create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
                 		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

             			  private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, String> call(Row row)
                               throws Exception {
                           String article_id = row.getString(0);
                           String article_time = String.valueOf(row.getInt(1));
                           return new Tuple2<String, String>(article_id, article_time);
                       }
                   }).collectAsMap();
                   
                            
                   HashMap<String, String> map1 = new HashMap<>();
                   //AppendToLogFile.appendFile("\r\n"+"2.去除已读..." ,Constants.LOGURL );
                       for (Entry<String, String> map : recommendList.entrySet()) {
                           String article_id = map.getKey();
                           if(!mapHadReadMap.containsKey(article_id)){
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
                       if (!dbRecommendMap.containsKey(key)) {
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
                           usersetsUser.setUser_id(user_ids); 
                           usersetsUser.setUserset_id(key);
                           arrayList2_3.add(usersetsUser);
                           //AppendToLogFile.appendFile("\r\n"+"2.for循环里单插结束标志..." ,Constants.LOGURL);
                       }
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
			         
      } //while end
	              
     }//foreach end
          }     
              
}

}
