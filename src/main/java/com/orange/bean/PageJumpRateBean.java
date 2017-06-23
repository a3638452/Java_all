package com.orange.bean;

import java.io.Serializable;

public class PageJumpRateBean implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String pagename;
	private String jump_rate;
	private String report_date;
	
	public String getPagename() {
		return pagename;
	}
	public void setPagename(String pagename) {
		this.pagename = pagename;
	}
	public String getJump_rate() {
		return jump_rate;
	}
	public void setJump_rate(String jump_rate) {
		this.jump_rate = jump_rate;
	}
	public String getReport_date() {
		return report_date;
	}
	public void setReport_date(String report_date) {
		this.report_date = report_date;
	}
	
	
}
