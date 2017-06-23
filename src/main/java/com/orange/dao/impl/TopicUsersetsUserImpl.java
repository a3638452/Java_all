package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.TopicUsersetsUser;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.TopicUsersetsUserDAO;



public class TopicUsersetsUserImpl implements TopicUsersetsUserDAO {

	@Override
	public void insert(TopicUsersetsUser topicUsersetsUser) {
		//执行操作数据库的sql
		String sql = "insert into t_user_topic_list(userset_id,user_id) values(?,?) "; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				topicUsersetsUser.getUserset_id(),
				topicUsersetsUser.getUser_id()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
	@Override
	public void insertBatch(List<TopicUsersetsUser> topicUsersetsUser) {

		//执行操作数据库的sql
		String sql = "insert into t_user_topic_list(userset_id,user_id) values(?,?) "; 
					
		//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (TopicUsersetsUser topicUsersetsUsers : topicUsersetsUser) {
					Object[] params = new Object[2];
					params[0] = topicUsersetsUsers.getUserset_id();
					params[1] = topicUsersetsUsers.getUser_id();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}

