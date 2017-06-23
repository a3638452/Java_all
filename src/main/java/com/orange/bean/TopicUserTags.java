package com.orange.bean;

import java.io.Serializable;


public class TopicUserTags implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String user_id;
	 private String user_tag;
	 private String s_level;
	 
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getUser_tag() {
		return user_tag;
	}
	public void setUser_tag(String user_tag) {
		this.user_tag = user_tag;
	}
	public String getS_level() {
		return s_level;
	}
	public void setS_level(String s_level) {
		this.s_level = s_level;
	}
	 
	
	
	 
	
	 
	 
}
