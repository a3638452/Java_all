package com.orange.service;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.scheduler.StreamingListener;

/**
 * e学实时文章推荐系统
 * @author Administrator
 *
 */
public class AllRecommendRun {

    public static void main(String[] args) throws InterruptedException {

           SparkConf conf = new SparkConf()
		                //.setMaster("local[6]")
				        .setAppName("AllRecommend")
				        .set("spark.driver.memory", "2g")
				        .set("spark.executor.memory", "2g")
				        .set("spark.default.parallelism", "30")
						.set("spark.storage.memoryFraction", "0.5")  
						.set("spark.shuffle.file.buffer", "64")  
						.set("spark.shuffle.memoryFraction", "0.3")    
						.set("spark.reducer.maxSizeInFlight", "24")  
						.set("spark.shuffle.io.maxRetries", "60")  
						.set("spark.shuffle.io.retryWait", "60")   
						.set("spark.sql.shuffle.partitions", "50")   
						.set("spark.streaming.kafka.maxRatePerPartition", "100")   
						.set("spark.streaming.unpersist", "true")  //这就让Spark来计算哪些RDD需要持久化,这样有利于提高GC的表现。
						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
						.registerKryoClasses(new Class[]{
								ArticleRecommends.class});   
          
   JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
   // 利用 checkpoint 来保留上一个窗口的状态，这样可以做到移动窗口的更新统计
   // jssc.checkpoint("hdfs://master:9000/realtime_logindatmeia_checkpoint");

   // 首先，要创建一份kafka参数map
   Map<String, Object> kafkaParams = new HashMap<>();
   kafkaParams.put("bootstrap.servers",
         "master:9092,slave1:9092,slave2:9092");
   kafkaParams.put("key.deserializer", StringDeserializer.class);
   kafkaParams.put("value.deserializer", StringDeserializer.class);
   kafkaParams.put("group.id", "spark_consumer_group3");
   kafkaParams.put("auto.offset.reset", "latest");
   kafkaParams.put("enable.auto.commit", false);
   // kafkaParams.put("partition.assignment.strategy", "range");
   Collection<String> topics = Arrays.asList("Article_Recommend");
   
   //sparkSession配置
   SparkSession spark = SparkSession
		   .builder()
		   //.master("local[6]")
		   .appName("AllRecommend")
		   .config("spark.sql.warehouse.dir", "/code/VersionTest/spark-warehouse")
		   .getOrCreate();
   // 创建输入DStream
   JavaInputDStream<ConsumerRecord<String, String>> inPutDStream = KafkaUtils.createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics,
                        kafkaParams));
   //kafka流数据的入口
   inPutDStream.repartition(30).foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String,String>>>() {

	private static final long serialVersionUID = 1L;

		@Override
        public void call(JavaRDD<ConsumerRecord<String, String>> lines) throws Exception {
			new ArticleRecommends().articleRecommends(spark, lines); //article
			new GroupTopicRecommend().groupTopicRecommend(spark, lines); //话题圈子类别推荐
			//new QuesAnsTopicRecommend().quesAnsTopicRecommend(spark, lines); //你问我答话题圈子类别推荐
			
		    }
	   });

   // 设置打印、启动、等待和关闭进程
    inPutDStream.print();
    jssc.start();
    jssc.awaitTermination();
       }
 }





