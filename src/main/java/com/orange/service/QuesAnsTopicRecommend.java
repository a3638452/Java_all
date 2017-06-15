package com.orange.service;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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

public class QuesAnsTopicRecommend implements Serializable{

	private static final long serialVersionUID = 1L;

public void quesAnsTopicRecommend(SparkSession spark ,JavaRDD<ConsumerRecord<String, String>> lines){
		
		List<String> questionInputList = lines.map(new Function<ConsumerRecord<String,String>, String>() {

			private static final long serialVersionUID = 1L;

					@Override
                    public String call(
                            ConsumerRecord<String, String> row)
                            throws Exception {
                        String[] split = row.value().split("\",\"");
                        if(split.length == 16){
                        String user_id = split[7];
                        String label = split[4];
                        String question_id = split[15].substring(0, split[15].length() - 1);
                        return user_id + "," +label + "," + question_id;
	                    }
	                        return "";
	                    }
	                }).collect();
		
		if(!questionInputList.isEmpty()){	//1.
			 String user_ids = new String();   // 用户id
             String label = new String();       //用户数据的标志，判断是否计算
             //String question_ids = new String();    //用户传入问题id

            for (String str : questionInputList) {
                String[] split = str.split(",");
                if(split.length == 3){
               	 	user_ids = split[0];
                    label = split[1];
                   // question_ids = split[2];
            
            
            //对传入kafka的各种数据进行正则匹配
            Pattern p=Pattern.compile("/api/v1/getAllOtherAnswerInfo");
            Matcher m=p.matcher(label);
            while(m.find()){	//2.
            
            //对传入kafka的各种数据进行正则匹配
                 //拆分标签，分条插入 
                 TopicUserTags topicUserTags = new TopicUserTags();
                 topicUserTags.setUser_id(user_ids);
                 topicUserTags.setUser_tag("你问我答");
                 topicUserTags.setS_level("1");
                if(user_ids!=null && !user_ids.equals("")){
                     TopicUserTagsDAO topicUserTagsDAO = DAOFactory.getTopicUserTagsDAO();
                     topicUserTagsDAO.insert(topicUserTags);
                	 }
//画像结束——————————————————————————————————————————————————————————————————————以下开始计算推荐的用户精彩答题_______________________________________________________________________
                                   
                  //拿到用户基本信息表
                  spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TOPIC_TAGS, Constants.JdbcConTest())
                  .createOrReplaceTempView("t_user_topic_tags");
                  //查出用户画像表中是否有这个类型的文章
                  Long num = spark.sql(" SELECT count(1) "
                  		+ "FROM t_user_topic_tags "
                  		+ "WHERE user_id = '"+user_ids+"' AND user_tag = '你问我答' ")
                       	.toJavaRDD().collect().get(0).getLong(0);   
                  
                  spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_PLAT_QUE, Constants.JdbcConTest())
                  .createOrReplaceTempView("t_plat_que");
                  
              if(num == 1 ){      //if = 1,画像表中不存在
            	  spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_PLAT_ANSWER, Constants.JdbcConTest())
                  .createOrReplaceTempView("t_plat_answer");   // 提问记录表    
            	  //查出推荐的精彩问题来 SELECT b.f_question_id ,a.s_create_time,COUNT(b.f_question_id) num 
            	 
                  Map<String, String> questionRecommendMap = spark.sql(" SELECT b.f_question_id ,"+
									                  		 "a.s_create_time,"+
									                  		 "COUNT(b.f_question_id) num "+
															 "FROM t_plat_que a,t_plat_answer b "+
															 "WHERE a.p_id=b.f_question_id AND b.s_update_time >FROM_UNIXTIME(UNIX_TIMESTAMP()-3456000,'yyyy-MM-dd 00:00:00') "+
															 "AND a.s_pic !='' AND a.s_pic IS NOT NULL "+
															 "GROUP BY f_question_id,a.s_create_time  "+
															 "HAVING num >=5")   // 7天 604800
                      		.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

								private static final long serialVersionUID = 1L;

							@Override
                               public Tuple2<String, String> call(Row row)
                                       throws Exception {
                                   String que_id = String.valueOf(row.getLong(0));
                                   String que_time = String.valueOf(row.getTimestamp(1));
                                   return new Tuple2<String, String>(que_id, que_time);
                               }
                           }).collectAsMap();
                  
                 //!!!!DB数据库中的推荐问题集合
                 spark.read().jdbc(Constants.URL_TEST_EXIAOXIN, Constants.T_USER_TOPIC_RECOMMEND, Constants.JdbcConTest())
                 .createOrReplaceTempView("t_user_topic_recommend");
                 
                 Map<String, String> dbRecommendMap = spark.sql("SELECT topic_id ,opt_type "
									                 		+ "FROM t_user_topic_recommend "
									                 		+ "WHERE create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00') "
									                 		+ "AND opt_type = '0'")
                		 .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

							private static final long serialVersionUID = 1L;

	    					@Override
	                         public Tuple2<String, String> call(Row row)
	                                 throws Exception {
	                             String que_id = row.getString(0);
	                             String opt_type = String.valueOf(row.getInt(1));
	                             return new Tuple2<String, String>(que_id, opt_type);
	                         }
	                     }).collectAsMap();

                  TopicRecommendSets topicRecommendSets = new TopicRecommendSets();   	   //推荐列表用户集合的bean
                  TopicUsersetsUser topicUsersetsUser = new TopicUsersetsUser();  	   	//推荐列表文章的bean
                 
                  //去除已推话题并将结果插入到mysql
                 Set<String> keySet = questionRecommendMap.keySet();
                 for (String key : keySet) {
                     String que_time = questionRecommendMap.get(key);
                     if (!dbRecommendMap.containsKey(key)) {
                    	 //recommend集合表
                    	 topicRecommendSets.setTopic_id(key);
                    	 topicRecommendSets.setUserset_id(key);
                    	 topicRecommendSets.setOpt_type("0");
                    	 topicRecommendSets.setArticle_time(que_time);
                         TopicRecommendSetsDAO topicRecommendSetsDAO = DAOFactory.getTopicRecommendSetsDAO();   
                         topicRecommendSetsDAO.insert(topicRecommendSets);
                         //user_list用户集合表
                         topicUsersetsUser.setUser_id(user_ids); 
                         topicUsersetsUser.setUserset_id(key);
                         TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
                         topicUsersetsUserDAO.insert(topicUsersetsUser);
                     }
                     else{
                    	 //user_list用户集合表
                    	 topicUsersetsUser.setUser_id(user_ids); 
                    	 topicUsersetsUser.setUserset_id(key);
                         TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
                         topicUsersetsUserDAO.insert(topicUsersetsUser);
                     } 
                }

             }//if的外层
               else if (num > 1){  //在画像中存在此tag，计算上一次画像时间到此刻的新文章，并入库前进行一次判断
                   //////System.out.println("num:"+num + "标签数≠1，近一天读过该类型的文章，查找上一次读过完后产生的文章推荐给他");
 //二.2_________________________________________________________________________________________________________________________________________________________________________________________________             
                   //查询出用户上一次的该文章类型阅读时间
                  Timestamp lastCreateTime = spark.sql("SELECT MAX(update_time) "
					                		   + "FROM t_user_topic_tags "
					                		   + "WHERE update_time < (SELECT MAX(update_time) FROM t_user_topic_tags) "
					                		   + "AND user_id = '"+user_ids+"' AND user_tag = '你问我答' ")
					                           .toJavaRDD().collect().get(0).getTimestamp(0);
                  //////System.out.println("lastCreateTime: "+lastCreateTime);
                 
                   //!!!!!!!!!查询出该时间段的所有推荐问题
                  Map<String, String> recommendMap =  spark.sql("SELECT p_id ,s_create_time "
                      		+ "FROM t_plat_que "
                      		+ "WHERE  s_pic !='' AND s_pic IS NOT NULL "
                      		+ "AND s_update_time > '"+lastCreateTime+"' ")
                      		.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

							private static final long serialVersionUID = 1L;

							@Override
	                        public Tuple2<String, String> call(
	                                Row row)
	                                throws Exception {
	
	                            return new Tuple2<String, String>(row.getString(0), String.valueOf(row.getTimestamp(1)));
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
                  
                  TopicRecommendSets topicRecommendSets = new TopicRecommendSets();   	   //推荐列表用户集合的bean
                  TopicUsersetsUser topicUsersetsUser = new TopicUsersetsUser();  	   	//推荐列表文章的bean
                 
                  Set<String> keySet = recommendMap.keySet();
                  for (String key : keySet) {
                      if (!dbRecommendMap.containsKey(key)) {
                          String que_time = recommendMap.get(key);
                          //话题推荐列表
                          topicRecommendSets.setTopic_id(key);
                          topicRecommendSets.setUserset_id(key);
                          topicRecommendSets.setOpt_type("0");
                          topicRecommendSets.setArticle_time(que_time);
                          //用户集合表
                          topicUsersetsUser.setUser_id(user_ids); 
                          topicUsersetsUser.setUserset_id(key);
                          //执行插入方法
                          TopicRecommendSetsDAO topicRecommendSetsDAO = DAOFactory.getTopicRecommendSetsDAO();
                          topicRecommendSetsDAO.insert(topicRecommendSets);
                          TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
                          topicUsersetsUserDAO.insert(topicUsersetsUser);
                      }
                      else{
                    	  topicUsersetsUser.setUser_id(user_ids); //用户集合表
                    	  topicUsersetsUser.setUserset_id(key);
                          TopicUsersetsUserDAO topicUsersetsUserDAO = DAOFactory.getTopicUsersetsUserDAO();
                          topicUsersetsUserDAO.insert(topicUsersetsUser);
                      }
                 }  
               }
            }
            	}//2.的外层
            }
		}
}

}
 
 
 
 
 

