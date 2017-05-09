package com.orange.bean;

import java.io.Serializable;

public class UserLifeCycle implements Serializable{

	private static final long serialVersionUID = 1L;
	private String f_user_id;
	private String s_xiaoxincode;
	private String s_account;
	private Long s_login_count;
	private Long s_use_time;
	private String s_register_time;
	private String s_create_time;
	private String s_report_date;
	private String s_update_time;
	
	public String getF_user_id() {
		return f_user_id;
	}
	public void setF_user_id(String f_user_id) {
		this.f_user_id = f_user_id;
	}
	public String getS_xiaoxincode() {
		return s_xiaoxincode;
	}
	public void setS_xiaoxincode(String s_xiaoxincode) {
		this.s_xiaoxincode = s_xiaoxincode;
	}
	public String getS_account() {
		return s_account;
	}
	public void setS_account(String s_account) {
		this.s_account = s_account;
	}
	public Long getS_login_count() {
		return s_login_count;
	}
	public void setS_login_count(Long s_login_count) {
		this.s_login_count = s_login_count;
	}
	public Long getS_use_time() {
		return s_use_time;
	}
	public void setS_use_time(Long s_use_time) {
		this.s_use_time = s_use_time;
	}
	public String getS_register_time() {
		return s_register_time;
	}
	public void setS_register_time(String s_register_time) {
		this.s_register_time = s_register_time;
	}
	public String getS_create_time() {
		return s_create_time;
	}
	public void setS_create_time(String s_create_time) {
		this.s_create_time = s_create_time;
	}
	public String getS_report_date() {
		return s_report_date;
	}
	public void setS_report_date(String s_report_date) {
		this.s_report_date = s_report_date;
	}
	public String getS_update_time() {
		return s_update_time;
	}
	public void setS_update_time(String s_update_time) {
		this.s_update_time = s_update_time;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	

	
	
	
	
	
}
