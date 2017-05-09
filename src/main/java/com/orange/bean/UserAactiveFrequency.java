package com.orange.bean;

import java.io.Serializable;

public class UserAactiveFrequency implements Serializable{

	private static final long serialVersionUID = 1L;
	private String f_province_id;
	private String f_city_id;
	private String f_area_id;
	private Long s_loyal_user;
	private Long s_continue_active;
	private Long s_backflow;
	private Long s_recent_lost;
	private String s_report_date;
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
	public Long getS_loyal_user() {
		return s_loyal_user;
	}
	public void setS_loyal_user(Long s_loyal_user) {
		this.s_loyal_user = s_loyal_user;
	}
	public Long getS_continue_active() {
		return s_continue_active;
	}
	public void setS_continue_active(Long s_continue_active) {
		this.s_continue_active = s_continue_active;
	}
	public Long getS_backflow() {
		return s_backflow;
	}
	public void setS_backflow(Long s_backflow) {
		this.s_backflow = s_backflow;
	}
	public Long getS_recent_lost() {
		return s_recent_lost;
	}
	public void setS_recent_lost(Long s_recent_lost) {
		this.s_recent_lost = s_recent_lost;
	}
	public String getS_report_date() {
		return s_report_date;
	}
	public void setS_report_date(String s_report_date) {
		this.s_report_date = s_report_date;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	

	
	
	
	
}
