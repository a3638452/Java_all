package com.orange.bean;

import java.io.Serializable;

/**
 * 页面切片转化率
 * @author Administrator
 *
 */
public class PageSplitConvertRate implements Serializable {

	private Long id;
	private String page_split;
	private String start_convert_rate;
	private String last_convert_rate;
	private String pv;
	private Long ave_durations;
	private String create_time;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getPage_split() {
		return page_split;
	}
	public void setPage_split(String page_split) {
		this.page_split = page_split;
	}
	public String getStart_convert_rate() {
		return start_convert_rate;
	}
	public void setStart_convert_rate(String start_convert_rate) {
		this.start_convert_rate = start_convert_rate;
	}
	public String getLast_convert_rate() {
		return last_convert_rate;
	}
	public void setLast_convert_rate(String last_convert_rate) {
		this.last_convert_rate = last_convert_rate;
	}
	public String getPv() {
		return pv;
	}
	public void setPv(String pv) {
		this.pv = pv;
	}
	public Long getAve_durations() {
		return ave_durations;
	}
	public void setAve_durations(Long ave_durations) {
		this.ave_durations = ave_durations;
	}
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	
	
	
	
	
	
	
	
}
