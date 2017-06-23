package com.orange.bean;

import java.io.Serializable;

public class ArticleSplitBean implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String user_id;
	 private String label;
	 private String article_id;
	 
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getArticle_id() {
		return article_id;
	}
	public void setArticle_id(String article_id) {
		this.article_id = article_id;
	}
	 
	 
}
