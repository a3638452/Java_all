package com.orange.bean;

import java.io.Serializable;

public class UserTags implements Serializable{
	private Long p_id;
	 private String user_id;
	 private String user_tag;
	 private String s_level;
	 private String create_time;
	 private String update_time;
	 
	public Long getP_id() {
		return p_id;
	}
	public void setP_id(Long p_id) {
		this.p_id = p_id;
	}
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
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	public String getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(String update_time) {
		this.update_time = update_time;
	}
	 
	
	
	 
	
	 
	 
}
