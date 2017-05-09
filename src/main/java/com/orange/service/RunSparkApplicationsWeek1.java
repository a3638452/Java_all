package com.orange.service;

import com.orange.dao.StaticsUserActiveFrequency;
import com.orange.dao.StaticsUserLoginWeek;
import com.orange.dao.StaticsUserModuleWeek;


public class RunSparkApplicationsWeek1 {
	/**
	 * 用户指标的run方法
	 * @author Administrator
	 *
	 */
	public static void main(String[] args) {

		//每周一跑的上周用户模块使用量、停留时长		 
		new StaticsUserModuleWeek().staticsUserModuleWeek();
		//上周用户登陆天数
		new StaticsUserLoginWeek().staticsUserLoginWeek();
		//每周一跑的区域用户声明周期指标
		new StaticsUserActiveFrequency().staticsUserActiveFrequency();

	}

}
