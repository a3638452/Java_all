package com.orange.bean;

import java.io.Serializable;

public class HomeJumpRate implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String report_date;
	private String exue_jump_rate;
	private String android_jump_rate;
	private String ios_jump_rate;
	public String getReport_date() {
		return report_date;
	}
	public void setReport_date(String report_date) {
		this.report_date = report_date;
	}
	public String getExue_jump_rate() {
		return exue_jump_rate;
	}
	public void setExue_jump_rate(String exue_jump_rate) {
		this.exue_jump_rate = exue_jump_rate;
	}
	public String getAndroid_jump_rate() {
		return android_jump_rate;
	}
	public void setAndroid_jump_rate(String android_jump_rate) {
		this.android_jump_rate = android_jump_rate;
	}
	public String getIos_jump_rate() {
		return ios_jump_rate;
	}
	public void setIos_jump_rate(String ios_jump_rate) {
		this.ios_jump_rate = ios_jump_rate;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	
}
