package com.orange.bean;

import java.io.Serializable;

public class ArticleUserSets implements Serializable{
	
		private Long  p_id;
		private String article_id;
		private String userset_id;
		private String recommend_type;
		private String article_time;
		 private String create_time;
		 private String Opt_type;
		 
		public Long getP_id() {
			return p_id;
		}
		public void setP_id(Long p_id) {
			this.p_id = p_id;
		}
		public String getArticle_id() {
			return article_id;
		}
		public void setArticle_id(String article_id) {
			this.article_id = article_id;
		}
		public String getUserset_id() {
			return userset_id;
		}
		public void setUserset_id(String userset_id) {
			this.userset_id = userset_id;
		}
		public String getRecommend_type() {
			return recommend_type;
		}
		public void setRecommend_type(String recommend_type) {
			this.recommend_type = recommend_type;
		}
		public String getArticle_time() {
			return article_time;
		}
		public void setArticle_time(String article_time) {
			this.article_time = article_time;
		}
		public String getCreate_time() {
			return create_time;
		}
		public void setCreate_time(String create_time) {
			this.create_time = create_time;
		}
		public String getOpt_type() {
			return Opt_type;
		}
		public void setOpt_type(String opt_type) {
			Opt_type = opt_type;
		}
		 
		 
		 
		 
		 
		 
		 

		 

}
