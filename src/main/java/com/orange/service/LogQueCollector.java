package com.orange.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.orange.sparkproject.dao.LogQuestionsDAO;
import com.orange.sparkproject.dao.factory.DAOFactory;
import com.orange.sparkproject.domain.LogQuestions;

 class LogQueCollector implements Serializable{

	private static final long serialVersionUID = 1L;

		 void logQueCollector(JavaRDD<ConsumerRecord<String, String>> lines) throws InterruptedException{

			List<String> inputList = lines.map(new Function<ConsumerRecord<String,String>, String>() {

				private static final long serialVersionUID = 1L;

						@Override
	                    public String call(
	                            ConsumerRecord<String, String> row)
	                            throws Exception {
	                        String[] split = row.value().split("\",\"");
	                        if(split.length == 16){
	                            String user_id = split[7];
	                            String label = split[4];
	                            String que_id = split[15].substring(0, split[15].length() - 1);
	                            String que_time = split[6];
	                            return user_id + "," +label + "," + que_id+ "," + que_time;
		                    }
		                        return "";
		                    }
		                }).collect();

	      if(!inputList.isEmpty()){
	      
	      for (String str : inputList) {
	          String[] split = str.split(",");
	          if(split.length == 4){
	        	  String  user_ids = split[0];
	        	  String  label = split[1];
	        	  String que_ids = split[2];
	        	  String que_times = split[3];
	      
	      
	      //对传入kafka的各种数据进行正则匹配
	      Pattern p=Pattern.compile("/api/v1/getAllOtherAnswerInfo*");
	      Matcher m=p.matcher(label);
	      while(m.find()){
	    	  ArrayList<LogQuestions> arrayList = new ArrayList<>();
	    	  LogQuestions logQuestions = new LogQuestions();
	    	  logQuestions.setUser_id(user_ids);
	    	  logQuestions.setQue_id(que_ids);
	    	  logQuestions.setQue_time(que_times);
	    	  arrayList.add(logQuestions);
	    	  if(user_ids!=null && !user_ids.equals("")){
	    		 LogQuestionsDAO logQuestionsDAO = DAOFactory.getLogQuestionsDAO();
	    		 logQuestionsDAO.insertBatch(arrayList);
	    	  }
	    		 
	      		}
	          }
	      }	
	     }
	}
	}

