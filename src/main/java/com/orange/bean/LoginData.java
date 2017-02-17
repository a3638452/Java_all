package com.orange.bean;

import java.io.Serializable;

public class LoginData implements Serializable{

	private Long id;
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
	private String create_time;
	
	public String getDevicescreen() {
		return devicescreen;
	}
	public void setDevicescreen(String devicescreen) {
		this.devicescreen = devicescreen;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
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
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	@Override
	public String toString() {
		return "LoginData [id=" + id + ", userid=" + userid + ", logintime="
				+ logintime + ", devicetype=" + devicetype + ", devicescreen="
				+ devicescreen + ", devicenetwork=" + devicenetwork
				+ ", province=" + province + ", city=" + city + ", area="
				+ area + ", streetarea=" + streetarea + ", lng=" + lng
				+ ", lat=" + lat + ", create_time=" + create_time + "]";
	}
	
	
	
}
