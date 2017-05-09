package com.orange.bean;

import java.io.Serializable;

public class LoginData implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String userid;
	private String logintime;
	private String devicetype;
	private String devicescreen;
	private String devicenetwork;
	private String province;
	private String city;
	private String area;
	private String streetarea;
	private String lng;
	private String lat;
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getLogintime() {
		return logintime;
	}
	public void setLogintime(String logintime) {
		this.logintime = logintime;
	}
	public String getDevicetype() {
		return devicetype;
	}
	public void setDevicetype(String devicetype) {
		this.devicetype = devicetype;
	}
	public String getDevicescreen() {
		return devicescreen;
	}
	public void setDevicescreen(String devicescreen) {
		this.devicescreen = devicescreen;
	}
	public String getDevicenetwork() {
		return devicenetwork;
	}
	public void setDevicenetwork(String devicenetwork) {
		this.devicenetwork = devicenetwork;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public String getStreetarea() {
		return streetarea;
	}
	public void setStreetarea(String streetarea) {
		this.streetarea = streetarea;
	}
	public String getLng() {
		return lng;
	}
	public void setLng(String lng) {
		this.lng = lng;
	}
	public String getLat() {
		return lat;
	}
	public void setLat(String lat) {
		this.lat = lat;
	}
	
	
	
	
}
