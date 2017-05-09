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

import com.orange.bean.LoginData;
import com.orange.dao.realTimeDAO;
import com.orange.dao.factory.DAOFactory;

public class RealTimeThermoDynamicDiagram {

	/**
	 * spark_sreaming整合kafka实时数据存储到mysql
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf()
			    //.setMaster("local[2]")
				.setAppName("RealTimeThermoDynamicDiagram")
				.set("spark.driver.memory", "2g")
				.set("spark.executor.memory", "1g")
				.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(2));
		// 利用 checkpoint 来保留上一个窗口的状态，这样可以做到移动窗口的更新统计
		// jssc.checkpoint("hdfs://master:9000/realtime_logindata_checkpoint");
		// 首先，要创建一份kafka参数map

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers","master:9092,slave1:9092,slave2:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark_consumer_group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("topic_spark");

		// 创建输入DStream
		JavaInputDStream<ConsumerRecord<String, String>> inPutDStream = KafkaUtils
				.createDirectStream(jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics,
								kafkaParams));

		LoginData loginData = new LoginData();
		// 将kafka传过来的数据持久化到mysql
		inPutDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(
					JavaRDD<ConsumerRecord<String, String>> lines)
					throws Exception {

				if (!lines.isEmpty()) {
					lines.foreach(new VoidFunction<ConsumerRecord<String, String>>() {

						private static final long serialVersionUID = 1L;

						@Override
						public void call(
								ConsumerRecord<String, String> tuple)
								throws Exception {
							String[] split_array = tuple.value().split("\n");
							for (int i = 0; i < split_array.length; i++) {
								String split_line = String.valueOf(split_array[i]);
								String[] split = split_line.split(",");
								if ((split.length == 11)
										&& (split[4].length() > 0)
										&& (split[6].length() > 0)
										&& (!split[6].equals("null"))
										&& (!split[6].equals("��e"))
										&& (!split[6].equals("��"))) {
									String userid = split[0].trim();
									String logintime = split[1];
									String devicetype = split[2];
									String devicescreen = split[3];
									String devicenetwork = split[4];
									String province = split[5];
									String city = split[6];
									String area = split[7];
									String streetarea = split[8];
									String lng = split[9];
									String lat = split[10];

									loginData.setUserid(userid);
									loginData.setLogintime(logintime);
									loginData.setDevicetype(devicetype);
									loginData.setDevicescreen(devicescreen);
									loginData.setDevicenetwork(devicenetwork);
									loginData.setProvince(province);
									loginData.setCity(city);
									loginData.setArea(area);
									loginData.setStreetarea(streetarea);
									loginData.setLng(lng);
									loginData.setLat(lat);
									realTimeDAO realTimeLoginData = DAOFactory.getRealTimeLoginData();
									realTimeLoginData.insert(loginData);

								}
							}
						}
					});

				}
			}
		});
		// 设置打印、启动、等待和关闭进程
		//inPutDStream.print();
		jssc.start();
		jssc.awaitTermination();

	}


}
