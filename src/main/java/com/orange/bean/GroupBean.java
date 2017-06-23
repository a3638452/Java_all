package com.orange.bean;

import java.io.Serializable;

public class GroupBean implements Serializable{
    
	private static final long serialVersionUID = 1L;
	private String user_id;
    private String topic_id;
    
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getTopic_id() {
		return topic_id;
	}
	public void setTopic_id(String topic_id) {
		this.topic_id = topic_id;
	}
    
}
