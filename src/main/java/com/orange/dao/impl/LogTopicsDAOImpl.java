package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.LogTopics;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.LogTopicsDAO;


public class LogTopicsDAOImpl implements LogTopicsDAO{

	@Override
	public void insert(LogTopics logTopics) {
		String sql = "insert into t_log_topics(user_id,topic_id,topic_time) values(?,?,?)";
		
		 Object[] params = new Object[]{
				logTopics.getUser_id(),
				logTopics.getTopic_id(),
				logTopics.getTopic_time()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
	@Override
	public void insertBatch(List<LogTopics> logTopics) {

		//执行操作数据库的sql
		String sql = "insert into t_log_topics(user_id,topic_id,topic_time) values(?,?,?)";
					
		//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (LogTopics logTopic : logTopics) {
					Object[] params = new Object[3];
					params[0] = logTopic.getUser_id();
					params[1] = logTopic.getTopic_id();
					params[2] = logTopic.getTopic_time();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}








