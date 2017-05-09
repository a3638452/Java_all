package com.orange.bean;

import java.io.Serializable;

public class livingStreaming implements Serializable{
	private String user_id;
	private String class_id;
	private String carme_id;
	private String start_time;
	private String end_time;
	private String use_time;
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getClass_id() {
		return class_id;
	}
	public void setClass_id(String class_id) {
		this.class_id = class_id;
	}
	public String getCarme_id() {
		return carme_id;
	}
	public void setCarme_id(String carme_id) {
		this.carme_id = carme_id;
	}
	public String getStart_time() {
		return start_time;
	}
	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}
	public String getEnd_time() {
		return end_time;
	}
	public void setEnd_time(String end_time) {
		this.end_time = end_time;
	}
	public String getUse_time() {
		return use_time;
	}
	public void setUse_time(String use_time) {
		this.use_time = use_time;
	}
	
	
	
}
