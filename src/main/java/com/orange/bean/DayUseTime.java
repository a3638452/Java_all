package com.orange.bean;

import java.io.Serializable;

public class DayUseTime implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String stay_time;
	private String user_count;
	private String report_date;
	public String getStay_time() {
		return stay_time;
	}
	public void setStay_time(String stay_time) {
		this.stay_time = stay_time;
	}
	public String getUser_count() {
		return user_count;
	}
	public void setUser_count(String user_count) {
		this.user_count = user_count;
	}
	public String getReport_date() {
		return report_date;
	}
	public void setReport_date(String report_date) {
		this.report_date = report_date;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	
}
