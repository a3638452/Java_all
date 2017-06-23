package com.orange.bean;

import java.io.Serializable;

public class UserBaseTags implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String s_user_id;
	private String s_user_type;
	private String f_province_id;
	private String f_city_id;
	private String f_area_id;
	private String s_school_type;
	private String s_report_date;
	
	public String getS_school_type() {
		return s_school_type;
	}
	public void setS_school_type(String s_school_type) {
		this.s_school_type = s_school_type;
	}
	public String getS_user_id() {
		return s_user_id;
	}
	public void setS_user_id(String s_user_id) {
		this.s_user_id = s_user_id;
	}
	public String getS_user_type() {
		return s_user_type;
	}
	public void setS_user_type(String s_user_type) {
		this.s_user_type = s_user_type;
	}
	public String getF_province_id() {
		return f_province_id;
	}
	public void setF_province_id(String f_province_id) {
		this.f_province_id = f_province_id;
	}
	public String getF_city_id() {
		return f_city_id;
	}
	public void setF_city_id(String f_city_id) {
		this.f_city_id = f_city_id;
	}
	public String getF_area_id() {
		return f_area_id;
	}
	public void setF_area_id(String f_area_id) {
		this.f_area_id = f_area_id;
	}
	public String getS_report_date() {
		return s_report_date;
	}
	public void setS_report_date(String s_report_date) {
		this.s_report_date = s_report_date;
	}
	
	

}
