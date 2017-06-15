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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class LogDataCollector {

	public static void main(String[] args) throws InterruptedException {

		   SparkConf conf = new SparkConf()
				     //.setMaster("local[3]")
	                 .setAppName("Logcollector")
//	                 .set("spark.driver.memory", "512m")
//	                 .set("spark.executor.memory", "512m")
//	                 .set("spark.yarn.executor.memoryOverhead", "2048")
	                 .set("spark.serializer","org.apache.spark.serializer.KryoSerializer"); 
JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.milliseconds(1800));
	// 利用 checkpoint 来保留上一个窗口的状态，这样可以做到移动窗口的更新统计
	// jssc.checkpoint("hdfs://master:9000/realtime_logindatmeia_checkpoint");
	
	//  首先，要创建一份kafka参数map
	Map<String, Object> kafkaParams = new HashMap<>();
	kafkaParams.put("bootstrap.servers",
	          "master:9092,slave1:9092,slave2:9092");
	kafkaParams.put("key.deserializer", StringDeserializer.class);
	kafkaParams.put("value.deserializer", StringDeserializer.class);
	kafkaParams.put("group.id", "spark_consumer_group2");
	kafkaParams.put("auto.offset.reset", "latest");
	kafkaParams.put("enable.auto.commit", false);
	// kafkaParams.put("partition.assignment.strategy", "range");
	Collection<String> topics = Arrays.asList("Article_Recommend");
	
	// 创建输入DStream
	JavaInputDStream<ConsumerRecord<String, String>> inPutDStream = KafkaUtils
	          .createDirectStream(jssc,
	                     LocationStrategies.PreferConsistent(),
	                     ConsumerStrategies.<String, String> Subscribe(topics,
	                                kafkaParams));
//kafka流数据的入口
inPutDStream.repartition(3).foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String,String>>>() {

	private static final long serialVersionUID = 1L;

	@Override
     public void call(JavaRDD<ConsumerRecord<String, String>> lines)
                throws Exception {
		
		new LogTopicsCollector().logTopicsCollector(lines); //清洗话题日志数据
		new LogQueCollector().logQueCollector(lines); //清洗你问我答提问日志数据
	}
		
  });
inPutDStream.print();
jssc.start();
jssc.awaitTermination();
	}
}