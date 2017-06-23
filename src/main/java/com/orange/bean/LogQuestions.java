package com.orange.bean;

import java.io.Serializable;

public class LogQuestions implements Serializable{

	private static final long serialVersionUID = 1L;
	private String user_id;
	private String que_id;
	private String que_time;
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getQue_id() {
		return que_id;
	}
	public void setQue_id(String que_id) {
		this.que_id = que_id;
	}
	public String getQue_time() {
		return que_time;
	}
	public void setQue_time(String que_time) {
		this.que_time = que_time;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
}
