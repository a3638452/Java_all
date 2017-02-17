package com.orange.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.orange.bean.LoginData;
import com.orange.dao.factory.DAOFactory;
import com.orange.dao.impl.realTimeDAOImpl;

@SuppressWarnings("all")
public class RealTimeThermoDynamicDiagram {

	/**
	 * spark_sreaming整合kafka实时数据存储到mysql
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf()
				// .setMaster("local[2]")
				.setAppName("RealTimeThermoDynamicDiagram")
				.set("spark.driver.memory", "2g")
				.set("spark.executor.memory", "1g")
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(2));
		// 利用 checkpoint 来保留上一个窗口的状态，这样可以做到移动窗口的更新统计
		// jssc.checkpoint("hdfs://master:9000/realtime_logindata_checkpoint");
		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",
				"192.168.0.132:9092,192.168.0.133:9092,192.168.0.134:9092");

		// 然后，要创建一个set，里面放入，你要读取的topic(可以并行读取多个topic)
		Set<String> topics = new HashSet<String>();
		topics.add("topic_spark_streaming");

		// 创建输入DStream
		JavaPairInputDStream<String, String> inPutDStream = KafkaUtils
				.createDirectStream(jssc, 
						String.class, 
						String.class,
						StringDecoder.class, 
						StringDecoder.class, 
						kafkaParams,
						topics);

		// 将kafka传过来的数据持久化到mysql
		persistDBfromKafka(inPutDStream);

		// 设置打印、启动、等待和关闭进程
		inPutDStream.print();
		jssc.start();
		jssc.awaitTermination();

	}

	/**
	 * 将kafka传过来的数据持久化到mysql
	 * 
	 * @param inPutDStream
	 */
	private static void persistDBfromKafka(
			JavaPairInputDStream<String, String> inPutDStream) {

		inPutDStream
				.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
					LoginData loginData = new LoginData();

					@Override
					public void call(JavaPairRDD<String, String> lines)
							throws Exception {
						if (!lines.isEmpty()) {
							lines.foreach(new VoidFunction<Tuple2<String, String>>() {

								@Override
								public void call(Tuple2<String, String> tuple)
										throws Exception {

									String[] split_array = tuple._2.split("\n");
									for (int i = 0; i < split_array.length; i++) {
										String split_line = String.valueOf(split_array[i]);
										String[] split = split_line.split(",");
										if ((split.length == 11)
												&& (split[4].length() > 0)
												&& (split[6].length() > 0)
												&& (!split[6].equals("null"))
												&& (!split[6].equals("��e"))
												&& (!split[6].equals("��"))) {
											String userid = split[0];
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
											realTimeDAOImpl realTimeLoginData = DAOFactory.getRealTimeLoginData();
											realTimeLoginData.insert(loginData);

										}
									}
								}
							});
						}
					}
				});

	}

}
