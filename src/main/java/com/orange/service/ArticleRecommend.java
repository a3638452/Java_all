package com.orange.service;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import com.orange.bean.ArticleUserSets;
import com.orange.bean.UserTags;
import com.orange.bean.UsersetsUser;
import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.dao.ArticleUserSetsDAO;
import com.orange.dao.UserTagsDAO;
import com.orange.dao.UsersetsUserDAO;
import com.orange.dao.factory.DAOFactory;


/**
 * 线上学堂实时推荐系统
 * @author Administrator
 *
 */
public class ArticleRecommend {

    public static void main(String[] args) throws InterruptedException {

           SparkConf conf = new SparkConf()
                  //.setMaster("local[2]")
                 .setAppName("ArticleRecommend")
                 .set("spark.driver.memory", "2g")
                 .set("spark.executor.memory", "1g")
                 .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
       JavaStreamingContext jssc = new JavaStreamingContext(conf,
                 Durations.seconds(1));
       // 利用 checkpoint 来保留上一个窗口的状态，这样可以做到移动窗口的更新统计
       // jssc.checkpoint("hdfs://master:9000/realtime_logindatmeia_checkpoint");

       //  首先，要创建一份kafka参数map
       Map<String, Object> kafkaParams = new HashMap<>();
       kafkaParams.put("bootstrap.servers",
                 "master:9092,slave1:9092,slave2:9092");
       kafkaParams.put("key.deserializer", StringDeserializer.class);
       kafkaParams.put("value.deserializer", StringDeserializer.class);
       kafkaParams.put("group.id", "spark_consumer_group");
       kafkaParams.put("auto.offset.reset", "latest");
       kafkaParams.put("enable.auto.commit", false);
       // kafkaParams.put("partition.assignment.strategy", "range");
       Collection<String> topics = Arrays.asList("Article_Recommend");

       SparkSession spark = SparkSession.builder().appName("ArticleRecommend").config("spark.sql.warehouse.dir", "/code/VersionTest/spark-warehouse").getOrCreate();
       // 创建输入DStream
       JavaInputDStream<ConsumerRecord<String, String>> inPutDStream = KafkaUtils
                 .createDirectStream(jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String> Subscribe(topics,
                                       kafkaParams));
       //kafka流数据的入口
       inPutDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String,String>>>() {

		private static final long serialVersionUID = 1L;

			@Override
            public void call(JavaRDD<ConsumerRecord<String, String>> lines)
                       throws Exception {
      //  一.画像___________________________________________________________以下是计算的入口__________________________________________________________________________________________________________________________________________

            	 List<String> inputList = lines.map(new Function<ConsumerRecord<String,String>, String>() {

					private static final long serialVersionUID = 1L;

						@Override
                     public String call(
                             ConsumerRecord<String, String> row)
                             throws Exception {
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

            	  //多标签处理
                 if(inputList.size()>0){
                 String article_ids = new String();    //用户传入文章id
                 String label = new String();    //用户数据的标志，判断是否计算
                 String user_ids = new String();       // 用户id
                 //多标签处理
                 for (String str : inputList) {
                     String[] split = str.split(",");
                     if(split.length == 3){
                     article_ids = split[0];
                     label = split[1];
                     user_ids = split[2];
                     }
                 }
               //正则匹配，精准处理相应数据
                 Pattern p=Pattern.compile("/v1/getAdv*");
                 Matcher m=p.matcher(label); //用户数据的标志，判断是否计算
                 while(m.find()){//如果匹配到标志，才执行处理

                	 //查询数据库获取<user_id,s_tag>
                       Dataset<Row> historyDF = spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_plat_send_history, Constants.JdbcCon());
                       historyDF.createOrReplaceTempView("t_plat_send_history");   // 文章和标签匹配表t_plat_send_history

                       //查出用户阅读文章的标签
                       List<Row> userList = spark.sql("select s_tag user_tag  from t_plat_send_history where p_id =  '"+article_ids+"'").toJavaRDD().collect();
                       String user_tagss = new String();
                     //把标签取出来放到字符串中，以后要对多标签进行遍历处理
                       for (Row row : userList) {
                          user_tagss = row.getString(0);
                       		}
                       
                         //拆分标签，分条插入 
                           String[] split = user_tagss.split(";");
                           for (int i = 0; i < split.length; i++) {    
                               String user_tag = split[i];
                               UserTags userTags = new UserTags();
                               userTags.setUser_id(user_ids);
                               userTags.setUser_tag(user_tag);
                               userTags.setS_level("1");
                               userTags.setCreate_time(DateUtils.formatTimeMinute(new java.util.Date()));
                               userTags.setUpdate_time(DateUtils.formatTimeMinute(new java.util.Date()));
                               if(user_ids!=null && !user_ids.equals("")){
                                    UserTagsDAO userTagimpl = DAOFactory.getUserTags();
                                     userTagimpl.insert(userTags);
                           }
                               //拿到用户基本信息表
                               spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_user_tags, Constants.JdbcCon())
                               .createOrReplaceTempView("t_user_tags");
                               //查出用户画像表中是否有这个类型的文章
                               List<Row> timeSignList = spark.sql(" select count(*) from t_user_tags where user_id = '"+user_ids+"' and user_tag = '"+user_tag+"' ")
                                       .toJavaRDD().collect();    //查出画像库中是否含有该用户的标签
                               Long num = timeSignList.get(0).getLong(0);   

                           if(num == 1 ){      //if = 1,画像表中不存在
   //二.1___________________________________________计算15天的文章，并过滤两次已读文章____________________________________________________________________________________________________________________________________________

                               spark.sql("select p_id , s_creater_time  "
                                       +"from t_plat_send_history where s_tag LIKE '%"+user_tag+"%' ")
                                       .createOrReplaceTempView("t_tags_temp");//查出用户所属的所有标签的推荐文章
                               ////!!文章推荐集合 ,查出15天内的用户所属的所有标签
                                Map<String, String> recommendList = spark.sql("select p_id,s_creater_time from t_tags_temp  where s_creater_time  > FROM_UNIXTIME(UNIX_TIMESTAMP()-1296000,'yyyy-MM-dd 00:00:00') ")
                                       .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

										private static final long serialVersionUID = 1L;

										@Override
                                        public Tuple2<String, String> call(Row row)
                                                throws Exception {
                                            String article_id = String.valueOf(row.getLong(0));
                                            String article_time = String.valueOf(row.getTimestamp(1));
                                            return new Tuple2<String, String>(article_id, article_time);
                                        }
                                    }).reduceByKeyLocally(new Function2<String, String, String>() {

										private static final long serialVersionUID = 1L;

										@Override
                                        public String call(String v1, String v2) throws Exception {

                                            return v1+v2;
                                        }
                                    });
                           //!!jdbc查询出用户已读文章，做成List
                          spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_plat_user_article_map, Constants.JdbcCon())
                          .createOrReplaceTempView("t_plat_user_article_map");
                         //!!jdbc查出的已读文章，做成map
                          Map<String, String> mapHadReadMap = spark.sql("select s_article_id , 0 as num from t_plat_user_article_map where s_user_id  = '"+user_ids+"' "  )
                        		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

									private static final long serialVersionUID = 1L;

							@Override
                                public Tuple2<String, String> call(Row row)
                                        throws Exception {
                                    String article_id = String.valueOf(row.getLong(0));
                                    String article_time = String.valueOf(row.getInt(1));
                                    return new Tuple2<String, String>(article_id, article_time);
                                }
                            }).reduceByKeyLocally(new Function2<String, String, String>() {

								private static final long serialVersionUID = 1L;

								@Override
                                   public String call(String v1, String v2) throws Exception {

                                       return v1+v2;
                                   }
                               });

                          //!!hdfs查出来的昨天已推荐
                        spark.read().parquet(Constants.PARQUET_PATH).createOrReplaceTempView("t_hdfs");
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
                                }).reduceByKeyLocally(new Function2<String, String, String>() {
                                	
									private static final long serialVersionUID = 1L;

											@Override
                                             public String call(String v1, String v2) throws Exception {
                                                  return v1+v2;
                                             }
                                         });

                         //!!!!DB查出的已推荐
                         spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_user_recommend, Constants.JdbcCon())
                         .createOrReplaceTempView("t_user_recommend");
                         Map<String, String> dbRecommendMap = spark.sql("select article_id ,recommend_type from t_user_recommend where create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
                        		 .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

									private static final long serialVersionUID = 1L;

							@Override
                             public Tuple2<String, String> call(Row row)
                                     throws Exception {
                                 String article_id = row.getString(0);
                                 String article_time = row.getString(1);
                                 return new Tuple2<String, String>(article_id, article_time);
                             }
                         }).reduceByKeyLocally(new Function2<String, String, String>() {

							private static final long serialVersionUID = 1L;

								@Override
                                public String call(String v1, String v2) throws Exception {

                                    return v1+v2;
                                }
                            });

                         HashMap<String, String> hadRecommendMap = new HashMap<String,String>();
                         hadRecommendMap.putAll(hdfsMap);
                         hadRecommendMap.putAll(dbRecommendMap);
                         
                          ArticleUserSets articleUserSets = new ArticleUserSets();   //推荐列表用户集合的bean
                          UsersetsUser usersetsUser = new UsersetsUser();            //推荐列表文章的bean
                          HashMap<String, String> map1 = new HashMap<>();

                          for (Entry<String, String> map : recommendList.entrySet()) {
                              String article_id = map.getKey();
                              if(!mapHadReadMap.containsKey(article_id)){
                                  map1.put(article_id, map.getValue());
                              }
                          } //去除已

                          
                         Set<String> keySet = map1.keySet();
                         for (String key : keySet) {
                             String article_time = map1.get(key);
                             if (!hadRecommendMap.containsKey(key)) {
                                  articleUserSets.setArticle_id(key);
                                  articleUserSets.setUserset_id(key);
                                  articleUserSets.setRecommend_type("0");
                                  articleUserSets.setArticle_time(article_time);
                                  ArticleUserSetsDAO tUserSets = DAOFactory.getTUserSets();   
                                  tUserSets.insert(articleUserSets);
                                  usersetsUser.setUser_id(user_ids); 
                                  usersetsUser.setUserset_id(key);
                                UsersetsUserDAO tUser1 = DAOFactory.getTUser(); 
                                  tUser1.insert(usersetsUser);
                             }
                             else{
                                 usersetsUser.setUser_id(user_ids); 
                                 usersetsUser.setUserset_id(key);
                                 UsersetsUserDAO tUser1 = DAOFactory.getTUser();  
                                 tUser1.insert(usersetsUser);
                             } 
                        }

                     }//if结尾
                       else if (num > 1){  //在画像中存在此tag，计算上一次画像时间到此刻的新文章，并入库前进行一次判断
 //二.2_________________________________________________________________________________________________________________________________________________________________________________________________             
                    	   //查询出用户上一次的该文章类型阅读时间
                          Timestamp lastCreateTime = spark.sql("SELECT MAX(update_time) FROM t_user_tags WHERE "
                                   + "update_time < (SELECT MAX(update_time) FROM t_user_tags) and user_id = '"+user_ids+"' and user_tag = '"+user_tag+"' ")
                                   .toJavaRDD().collect().get(0).getTimestamp(0);
                          //!!!!!!!!!查询出该时间段的所有文章资源,变成rdd
                          spark.sql("select p_id , s_creater_time  from t_plat_send_history "
                                   + "where s_tag LIKE '%"+user_tag+"%' ").createOrReplaceTempView("t_all_tag_tmpl");

                           Map<String, String> recommendList = spark.sql("select p_id article_id, s_creater_time  from t_all_tag_tmpl "                         
                                   + " where s_creater_time >  '" + lastCreateTime + "' ")
                                       .toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

										private static final long serialVersionUID = 1L;

										@Override
                                        public Tuple2<String, String> call(
                                                Row row)
                                                throws Exception {

                                            return new Tuple2<String, String>(String.valueOf(row.getLong(0)), String.valueOf(row.getTimestamp(1)));
                                        }
                                    }).reduceByKeyLocally(new Function2<String, String, String>() {

										private static final long serialVersionUID = 1L;

										@Override
                                        public String call(String v1, String v2) throws Exception {
                                            return v1+v2;
                                        }
                                    });

                           Dataset<Row> rddDSet = spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_plat_user_article_map, Constants.JdbcCon());
                               rddDSet.createOrReplaceTempView("t_plat_user_article_map");  
                           //!!!!jdbc查出已读，做成Map
                          Map<String, String> mapHadReadMap = spark.sql("select s_article_id , 0 as num from t_plat_user_article_map where s_user_id  = '"+user_ids+"' "  )
                        		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

									private static final long serialVersionUID = 1L;

							@Override
                                public Tuple2<String, String> call(Row row)
                                        throws Exception {
                                    String article_id = String.valueOf(row.getLong(0));
                                    String article_time = String.valueOf(row.getInt(1));
                                    return new Tuple2<String, String>(article_id, article_time);
                                }
                            }).reduceByKeyLocally(new Function2<String, String, String>() {

								private static final long serialVersionUID = 1L;

								@Override
                                   public String call(String v1, String v2) throws Exception {

                                       return v1+v2;
                                   }
                               });

                          //!!!!DB查出的已读，做成Map
                          spark.read().jdbc(Constants.URL_EXIAOXIN, Constants.t_user_recommend, Constants.JdbcCon())
                          .createOrReplaceTempView("t_user_recommend");
                          Map<String, String> dbRecommendMap = spark.sql("select article_id ,recommend_type from t_user_recommend where create_time > FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd 00:00:00')")
                        		  .javaRDD().mapToPair(new PairFunction<Row, String, String>() {

									private static final long serialVersionUID = 1L;

							@Override
                              public Tuple2<String, String> call(Row row)
                                      throws Exception {
                                  String article_id = row.getString(0);
                                  String article_time = row.getString(1);
                                  return new Tuple2<String, String>(article_id, article_time);
                              }
                          }).reduceByKeyLocally(new Function2<String, String, String>() {

							private static final long serialVersionUID = 1L;

								@Override
                                 public String call(String v1, String v2) throws Exception {

                                     return v1+v2;
                                 }
                             });
                          
                          ArticleUserSets articleUserSets = new ArticleUserSets();   
                          UsersetsUser usersetsUser = new UsersetsUser();            
                          HashMap<String, String> map1 = new HashMap<>();

                              for (Entry<String, String> map : recommendList.entrySet()) {
                                  String article_id = map.getKey();
                                  if(!mapHadReadMap.containsKey(article_id)){
                                      map1.put(article_id, map.getValue());
                                  }
                          }  

                          Set<String> keySet = map1.keySet();
                          for (String key : keySet) {
                              if (!dbRecommendMap.containsKey(key)) {
                                  String article_time = map1.get(key);
                                   articleUserSets.setArticle_id(key);
                                   articleUserSets.setUserset_id(key);
                                   articleUserSets.setRecommend_type("0");
                                   articleUserSets.setArticle_time(article_time);
                                   usersetsUser.setUser_id(user_ids); 
                                   usersetsUser.setUserset_id(key);
                                   
                                    ArticleUserSetsDAO tUserSets = DAOFactory.getTUserSets();
                                   tUserSets.insert(articleUserSets);
                                   UsersetsUserDAO tUser1 = DAOFactory.getTUser();
                                   tUser1.insert(usersetsUser);
                              }
                              else{
                                  usersetsUser.setUser_id(user_ids); 
                                  usersetsUser.setUserset_id(key);
                                   UsersetsUserDAO tUser1 = DAOFactory.getTUser();
                                  tUser1.insert(usersetsUser);
                              }
                         }
                    }
                 }
             } //while end
            }//foreach end
           }//if end
       });//foreach end

 //______________________________________________________________________________main的结尾__________________________________________________________________________________________     

            inPutDStream.print();
            jssc.start();
            jssc.awaitTermination();
       }
 }





