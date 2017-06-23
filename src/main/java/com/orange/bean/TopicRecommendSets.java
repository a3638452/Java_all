package com.orange.bean;

import java.io.Serializable;


public class TopicRecommendSets implements Serializable{
	
	private static final long serialVersionUID = 1L;
		private String topic_id;
		private String userset_id;
		private String opt_type;
		private String article_time;
		
		public String getTopic_id() {
			return topic_id;
		}
		public void setTopic_id(String topic_id) {
			this.topic_id = topic_id;
		}
		public String getUserset_id() {
			return userset_id;
		}
		public void setUserset_id(String userset_id) {
			this.userset_id = userset_id;
		}
		public String getOpt_type() {
			return opt_type;
		}
		public void setOpt_type(String opt_type) {
			this.opt_type = opt_type;
		}
		public String getArticle_time() {
			return article_time;
		}
		public void setArticle_time(String article_time) {
			this.article_time = article_time;
		}
		 
		 
		 
		 
		 
		 
		 
		 

		 

}
