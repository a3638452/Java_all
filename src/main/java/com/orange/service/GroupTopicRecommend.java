package com.orange.service;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import com.orange.sparkproject.constant.Constants;
import com.orange.sparkproject.dao.TopicRecommendSetsDAO;
import com.orange.sparkproject.dao.TopicUserTagsDAO;
import com.orange.sparkproject.dao.TopicUsersetsUserDAO;
import com.orange.sparkproject.dao.factory.DAOFactory;
import com.orange.sparkproject.domain.TopicRecommendSets;
import com.orange.sparkproject.domain.TopicUserTags;
import com.orange.sparkproject.domain.TopicUsersetsUser;

 public class GroupTopicRecommend implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public void groupTopicRecommend(SparkSession spark ,JavaRDD<ConsumerRecord<String, String>> lines){
		ArrayList<String> inputList = new ArrayList<String>();
		try {
			 List<String> input = lines.map(new Function<ConsumerRecord<String,String>, String>() {

					private static final long serialVersionUID = 1L;

							@Override
		                    public String call(
		                            ConsumerRecord<String, String> row)
		                            throws Exception {
		                        String[] split = row.value().split("\",\"");
		                        if(split.length == 15){
		                        String user_id = split[7];
		                        String label = split[4];
		                        String topic_id = split[4].substring(18, split[4].length());
		                        return user_id + "," +label + "," + topic_id;
			                    }
			                        return "";
			                    }
			                }).collect();
			 inputList.addAll(input);
		} catch (Exception e) {
			 e.printStackTrace();
		}
		

         if(!inputList.isEmpty()){
	         String topic_ids = new String();    //用户传入文章id
	         String label = new String();       //用户数据的标志，判断是否计算
	         String user_ids = new String();   // 用户id

	         for (String str : inputList) {
	             String[] split = str.split(",");
	             if(split.length == 3){
	            	 user_ids = split[0];
	                 label = split[1];
	                 topic_ids = split[2];
                 
		         //对传入kafka的各种数据进行正则匹配
		         Pattern p=Pattern.compile("/api/v2/topics/my/*");
		         Matcher m=p.matcher(label);
		         while(m.find()){
		
		         //查询数据库获取<user_id,s_tag>
		        spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_DISC_TOPIC, Constants.JdbcConTest())
		        .createOrReplaceTempView("t_disc_topic");   
		        spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_DISC_GROUP, Constants.JdbcConTest())
		        .createOrReplaceTempView("t_disc_group");
		        //拿到圈子类型标签（原始标签是用空格分开的）
		        List<Row> userList = spark.sql("SELECT b.s_group_key "+
												" FROM t_disc_topic a,t_disc_group b"+
												" WHERE a.f_group_id=b.p_id AND a.p_id= '"+topic_ids+"'")
												.toJavaRDD()
												.collect();
		        String user_tagss = new String();
		        for (Row row : userList) {
		           user_tagss = row.getString(0);
		        }	
		        ArrayList<TopicUserTags> arrayList_tag = new ArrayList<>();
		        TopicUserTags topicUserTags = new TopicUserTags();
		            String[] splits = user_tagss.split(" ");
		            for (int i = 0; i < splits.length; i++) {    //拆分标签，分条插入
		                String user_tag = splits[i];
		                topicUserTags.setUser_id(user_ids);
		                topicUserTags.setUser_tag(user_tag);
		                topicUserTags.setS_level("1");
		                arrayList_tag.add(topicUserTags);
		                if(user_ids!=null && !user_ids.equals("")){
		                     TopicUserTagsDAO topicUserTagsDAO = DAOFactory.getTopicUserTagsDAO();
		                     topicUserTagsDAO.insertBatch(arrayList_tag);
		            }
		
		               spark.read()
		               .jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TOPIC_TAGS, Constants.JdbcConTest())
		               .createOrReplaceTempView("t_user_topic_tags");
		                List<Row> timeSignList = spark.sql(" SELECT count(1) "
								                       		+ "FROM t_user_topic_tags "
								                       		+ "WHERE user_id = '"+user_ids+"' and user_tag = '"+user_tag+"' ")
		                        						 .toJavaRDD().collect();   //查出画像库中是否含有该用户的标签
		                Long num = timeSignList.get(0).getLong(0);   //if = 1,表中不存在;if>1 ，表中存在
		                //////System.out.println("num:"+num + "  标签数=1，近一天未读过该类型的文章，查找近15天的文章推荐给他");
		
		            if(num == 1 ){      //if = 1,画像表中不存在
		                    //二.1____________计算7天的近需话题给他，并过滤两次已看话题___________________________________________________________________________________________________________________________________________________________________________
		         	   
		              ////!!话题推荐Map ,查出该用户所属圈子类别的最新7天的话题来
		                 Map<String, String> recommendMap = spark.sql("SELECT a.p_id,a.s_create_time "+
																		" FROM t_disc_topic a,t_disc_group b"+
																		" WHERE a.f_group_id=b.p_id "   //7天是604800      
																		+ "AND a.s_create_time  >= FROM_UNIXTIME(UNIX_TIMESTAMP()-4320000,'yyyy-MM-dd 00:00:00') "
																		+ "AND b.s_group_key LIKE '%"+user_tag+"%' ")
		                        .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
									private static final long serialVersionUID = 1L;
		
									@Override
		                         public Tuple2<String, String> call(Row row)
		                                 throws Exception {
		                             String topic_id = row.getString(0);
		                             String topic_time = String.valueOf(row.getTimestamp(1));
		                             return new Tuple2<String, String>(topic_id, topic_time);
		                         }
		                     }).collectAsMap();
		
		           spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_DISC_GROUP_USER_MAP, Constants.JdbcConTest())
		           .createOrReplaceTempView("t_disc_group_user_map");
		          //!!jdbc查询出用户已读话题
		           Map<String, String> mapHadReadMap = spark.sql("SELECT f_topic_id , 0 as num "
		           											+ "FROM t_disc_group_user_map "
		           											+ "WHERE f_user_id  = '"+user_ids+"' "  )
		         		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
								private static final long serialVersionUID = 1L;
		
						@Override
		                 public Tuple2<String, String> call(Row row)
		                         throws Exception {
		                     String recommend_topic_id = row.getString(0);
		                     String topic_time = String.valueOf(row.getInt(1));
		                     return new Tuple2<String, String>(recommend_topic_id, topic_time);
		                 }
		             }).collectAsMap();
		
		          //!!!!DB数据库中的推荐话题集合
		          spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TOPIC_RECOMMEND, Constants.JdbcConTest())
		          .createOrReplaceTempView("t_user_topic_recommend");
		          Map<String, String> dbRecommendMap = spark.sql("SELECT topic_id ,opt_type "
										                 		+ "FROM t_user_topic_recommend "
										                 		+ "WHERE create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00') AND opt_type = '0'")
		         		 .javaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
								private static final long serialVersionUID = 1L;
		
						@Override
		              public Tuple2<String, String> call(Row row)
		                      throws Exception {
		                  String topic_id = row.getString(0);
		                  String opt_type = String.valueOf(row.getInt(1));
		                  return new Tuple2<String, String>(topic_id, opt_type);
		              }
		          }).collectAsMap();
		
		          //未推的
		          HashMap<String, String> hadRecommendMap = new HashMap<String,String>();
		          // hadRecommendMap.putAll(hdfsMap);
		          hadRecommendMap.putAll(dbRecommendMap);
		          
		             	   	//推荐列表文章的bean
		           HashMap<String, String> map1 = new HashMap<>();					  //存储未读的话题Map
		          
		           for (Entry<String, String> map : recommendMap.entrySet()) {
		               String topic_id = map.getKey();
		               if(!mapHadReadMap.containsKey(topic_id)){
		                   map1.put(topic_id, map.getValue());
		               }
		           } //去除已读和历史的推荐文章 操作
		
		           ArrayList<TopicRecommendSets> arrayList1_1 = new ArrayList<>();
		           ArrayList<TopicUsersetsUser> arrayList1_2 = new ArrayList<>();
		           ArrayList<TopicUsersetsUser> arrayList1_3 = new ArrayList<>();
		           //去除已推话题并将结果插入到mysql
		          Set<String> keySet = map1.keySet();
		          for (String key : keySet) {
		        	  TopicRecommendSets topicRecommendSets = new TopicRecommendSets();   	   //推荐列表用户集合的bean
		              TopicUsersetsUser topicUsersetsUser = new TopicUsersetsUser();
		              String topic_time = map1.get(key);
		              if (!hadRecommendMap.containsKey(key)) {
		             	//recommend集合表
		             	 topicRecommendSets.setTopic_id(key);
		             	 topicRecommendSets.setUserset_id(key);
		             	 topicRecommendSets.setOpt_type("0");
		             	 topicRecommendSets.setArticle_time(topic_time);
		             	 arrayList1_1.add(topicRecommendSets);
		                 //user_list用户集合表
		                  topicUsersetsUser.setUser_id(user_ids); 
		                  topicUsersetsUser.setUserset_id(key);
		                  TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
		                  topicUsersetsUserDAO.insert(topicUsersetsUser);
		                  arrayList1_2.add(topicUsersetsUser);
		              }
		              else{
		             	//user_list用户集合表
		             	 topicUsersetsUser.setUser_id(user_ids); 
		             	 topicUsersetsUser.setUserset_id(key);
		             	arrayList1_3.add(topicUsersetsUser);
		              } 
		         }
		          TopicRecommendSetsDAO topicRecommendSetsDAO = DAOFactory.getTopicRecommendSetsDAO();   
		          topicRecommendSetsDAO.insertBatch(arrayList1_1);
		          TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
		          topicUsersetsUserDAO.insertBatch(arrayList1_2);
		          
		          TopicUsersetsUserDAO topicUsersetsUserDAO_2 = DAOFactory.getTopicUsersetsUserDAO();
		          topicUsersetsUserDAO_2.insertBatch(arrayList1_3);
		          
		
		      }//if的外层
		        else if (num > 1){  //在画像中存在此tag，计算上一次画像时间到此刻的新文章，并入库前进行一次判断
		            //////System.out.println("num:"+num + "标签数≠1，近一天读过该类型的文章，查找上一次读过完后产生的文章推荐给他");
		            //查询出用户上一次的该文章类型阅读时间
		           Timestamp lastCreateTime = spark.sql("SELECT MAX(update_time) "
						                		   + "FROM t_user_topic_tags "
						                		   + "WHERE update_time < (SELECT MAX(update_time) FROM t_user_topic_tags) AND user_id = '"+user_ids+"' AND user_tag = '"+user_tag+"' ")
						                           .toJavaRDD().collect().get(0).getTimestamp(0);
		           //////System.out.println("lastCreateTime: "+lastCreateTime);
		            //!!!!!!!!!查询出该时间段的所有文章资源,编程rdd
		           spark.sql("SELECT a.p_id,a.s_create_time "+
							    "FROM t_disc_topic a,t_disc_group b "+
							    "WHERE a.f_group_id=b.p_id AND a.s_create_time > '"+lastCreateTime+"' AND b.s_group_key LIKE '%"+user_tag+"%' ")
		                     .createOrReplaceTempView("t_all_tag_tmpl");
		           
		           //推荐的话题Map
		            Map<String, String> recommendMap = spark.sql("SELECT p_id topic_id, s_create_time  "
		         		   										+ "FROM t_all_tag_tmpl "                         
		         		   									    + "WHERE s_create_time >  '" + lastCreateTime + "' ")
				   									    .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
															private static final long serialVersionUID = 1L;
		
												@Override
		                                     public Tuple2<String, String> call(
		                                             Row row)
		                                             throws Exception {
		
		                                         return new Tuple2<String, String>(row.getString(0), String.valueOf(row.getTimestamp(1)));
		                                     }
		                                 }).collectAsMap();
		
		            Dataset<Row> rddDSet = spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_DISC_GROUP_USER_MAP, Constants.JdbcConTest());
		                rddDSet.createOrReplaceTempView("t_disc_group_user_map");  // jdbc获取用户已读文章表  s_user_id = '"+userid+"' and
		            //!!!!jdbc查询出该用户已读文章，做成RDD
		           Map<String, String> mapHadReadMap = spark.sql("SELECT f_topic_id , 0 as num "
										                  		+ "FROM t_disc_group_user_map "
										                  		+ "WHERE f_user_id  = '"+user_ids+"' "  )
		           		.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
								private static final long serialVersionUID = 1L;
		
						@Override
		                 public Tuple2<String, String> call(Row row)
		                         throws Exception {
		                     String topic_id = row.getString(0);
		                     String num = String.valueOf(row.getInt(1));
		                     return new Tuple2<String, String>(topic_id, num);
		                 }
		             }).collectAsMap();
		           
		           spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TOPIC_RECOMMEND, Constants.JdbcConTest())
		           .createOrReplaceTempView("t_user_topic_recommend");
		         //!!!!DB数据库中的推荐表集合
		           Map<String, String> dbRecommendMap = spark.sql("SELECT topic_id ,opt_type "
											                  		+ "FROM t_user_topic_recommend "
											                  		+ "WHERE create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00') ")
		           		.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
		
								private static final long serialVersionUID = 1L;
		
						@Override
		               public Tuple2<String, String> call(Row row)
		                       throws Exception {
		                   String topic_id = row.getString(0);
		                   String opt_type = String.valueOf(row.getInt(1));
		                   return new Tuple2<String, String>(topic_id, opt_type);
		               }
		           }).collectAsMap();
		           
		         
		           HashMap<String, String> map1 = new HashMap<>();
		          
		               for (Entry<String, String> map : recommendMap.entrySet()) {
		                   String article_id = map.getKey();
		                   if(!mapHadReadMap.containsKey(article_id)){
		                       map1.put(article_id, map.getValue());
		                   }
		           }  //去除已读和历史的推荐文章 操作
		           ArrayList<TopicRecommendSets> arrayList2_1 = new ArrayList<>();
		           ArrayList<TopicUsersetsUser> arrayList2_2 = new ArrayList<>();
		           ArrayList<TopicUsersetsUser> arrayList2_3 = new ArrayList<>();
		           Set<String> keySet = map1.keySet();
		           for (String key : keySet) {
		        	   TopicRecommendSets topicRecommendSets = new TopicRecommendSets();   	   //推荐列表用户集合的bean
		               TopicUsersetsUser topicUsersetsUser = new TopicUsersetsUser();  	   	//推荐列表文章的bean
		               if (!dbRecommendMap.containsKey(key)) {
		                   String article_time = map1.get(key);
		                   //话题推荐列表
		                   topicRecommendSets.setTopic_id(key);
		                   topicRecommendSets.setUserset_id(key);
		                   topicRecommendSets.setOpt_type("0");
		                   topicRecommendSets.setArticle_time(article_time);
		                   arrayList2_1.add(topicRecommendSets);
		                   //用户集合表
		                   topicUsersetsUser.setUser_id(user_ids); 
		                   topicUsersetsUser.setUserset_id(key);
		                   arrayList2_2.add(topicUsersetsUser);
		               }
		               else{
		             	  topicUsersetsUser.setUser_id(user_ids); //用户集合表
		             	  topicUsersetsUser.setUserset_id(key);
		             	 arrayList2_3.add(topicUsersetsUser);
		               }
		          } 
		         //执行插入方法
		           TopicRecommendSetsDAO topicRecommendSetsDAO = DAOFactory.getTopicRecommendSetsDAO();
		           topicRecommendSetsDAO.insertBatch(arrayList2_1);
		           TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
		           topicUsersetsUserDAO.insertBatch(arrayList2_2);
		           
		           TopicUsersetsUserDAO topicUsersetsUserDAO_2 = DAOFactory.getTopicUsersetsUserDAO();
		           topicUsersetsUserDAO_2.insertBatch(arrayList2_3);
		     }
		  }
          }
              }
		
         }
         }
	}

}
