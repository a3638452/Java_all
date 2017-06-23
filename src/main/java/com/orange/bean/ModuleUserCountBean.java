package com.orange.bean;

import java.io.Serializable;

public class ModuleUserCountBean implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String s_report_date;
	private String module;
	private String user_count;
	private String next_day;
	private String three_day;
	private String seven_day;
	private String fourteen_day;
	private String thirty_day;
	
	public String getS_report_date() {
		return s_report_date;
	}
	public void setS_report_date(String s_report_date) {
		this.s_report_date = s_report_date;
	}
	public String getModule() {
		return module;
	}
	public void setModule(String module) {
		this.module = module;
	}
	public String getUser_count() {
		return user_count;
	}
	public void setUser_count(String user_count) {
		this.user_count = user_count;
	}
	public String getNext_day() {
		return next_day;
	}
	public void setNext_day(String next_day) {
		this.next_day = next_day;
	}
	public String getThree_day() {
		return three_day;
	}
	public void setThree_day(String three_day) {
		this.three_day = three_day;
	}
	public String getSeven_day() {
		return seven_day;
	}
	public void setSeven_day(String seven_day) {
		this.seven_day = seven_day;
	}
	public String getFourteen_day() {
		return fourteen_day;
	}
	public void setFourteen_day(String fourteen_day) {
		this.fourteen_day = fourteen_day;
	}
	public String getThirty_day() {
		return thirty_day;
	}
	public void setThirty_day(String thirty_day) {
		this.thirty_day = thirty_day;
	}
	
	
	
	
	
}
