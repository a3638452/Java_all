package com.orange.bean;


import java.io.Serializable;


public class ArticleBean implements Serializable{

	private static final long serialVersionUID = 1L;
	private String article_id;
	private String user_id;
	
	public String getArticle_id() {
		return article_id;
	}
	public void setArticle_id(String article_id) {
		this.article_id = article_id;
	}
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	
	
}
