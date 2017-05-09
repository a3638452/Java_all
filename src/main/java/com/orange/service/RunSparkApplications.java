package com.orange.service;

import com.orange.dao.QueAnswer;
import com.orange.dao.StatisticsUserInforLoginTime;
import com.orange.dao.StatisticsUserModules;
import com.orange.dao.UserBaseInfoTags;


/**
 * 用户指标的run方法
 * @author Administrator
 *
 */
public class RunSparkApplications {

	public static void main(String[] args) {
		 
		 new QueAnswer().getQqueAnswer();  //你问我答每天答题量统计 
		 new StatisticsUserInforLoginTime().statisticsUserInforLoginTime();   //用户登陆时长
		 new StatisticsUserModules().statisticsUserModules();   //用户模块使用时长
		 new UserBaseInfoTags().userBaseInfoTags(); //用户基本信息画像
	}
	
}
