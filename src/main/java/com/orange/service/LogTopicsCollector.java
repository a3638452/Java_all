package com.orange.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.orange.sparkproject.dao.LogTopicsDAO;
import com.orange.sparkproject.dao.factory.DAOFactory;
import com.orange.sparkproject.domain.LogTopics;

 class LogTopicsCollector implements Serializable{

	private static final long serialVersionUID = 1L;

	 void logTopicsCollector(JavaRDD<ConsumerRecord<String, String>> lines) throws InterruptedException{

		List<String> inputList = lines.map(new Function<ConsumerRecord<String,String>, String>() {

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
                        String topic_time = split[6];
                        return user_id + "," +label + "," + topic_id + "," +topic_time;
	                    }
	                        return "";
	                    }
	                }).collect();

      if(!inputList.isEmpty()){
      
	      for (String str : inputList) {
	          String[] split = str.split(",");
	          if(split.length == 4){
	        	  String user_ids = split[0];
	        	  String label = split[1];
	        	  String topic_ids = split[2];
	        	  String topic_times = split[3];
	      
	      //对传入kafka的各种数据进行正则匹配
	      Pattern p=Pattern.compile("/api/v2/topics/my/*");
	      Matcher m=p.matcher(label);
	      while(m.find()){
	    	  ArrayList<LogTopics> arrayList = new ArrayList<>();
	    	  LogTopics logTopics = new LogTopics();
	    	  logTopics.setUser_id(user_ids);
	    	  logTopics.setTopic_id(topic_ids);
	    	  logTopics.setTopic_time(topic_times);
	    	  arrayList.add(logTopics);
	    	  if(user_ids!=null && !user_ids.equals("")){
	    		  LogTopicsDAO logTopicsDAO = DAOFactory.getLogTopicsDAO();
	    		  logTopicsDAO.insertBatch(arrayList);
	    		  
	    	  }
	      	}
		  }  
	      }
     }
	}
}
