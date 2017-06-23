package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.TopicUserTags;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.TopicUserTagsDAO;


public class TopicUserTagsImpl implements TopicUserTagsDAO {


	@Override
	public void insert(TopicUserTags topicUserTags) {
		//执行操作数据库的sql
				String sql = " insert into t_user_topic_tags(user_id,user_tag,s_level) values(?,?,?) "; 
				//实例化数据结构数组
				Object[] params = new Object[]{
						
						topicUserTags.getUser_id(),
						topicUserTags.getUser_tag(),
						topicUserTags.getS_level()
						
				};
				
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();
				jdbcHelper.executeUpdate(sql, params);
		
	}
	
	@Override
	public void insertBatch(List<TopicUserTags> topicUserTags) {

		//执行操作数据库的sql
		String sql = " insert into t_user_topic_tags(user_id,user_tag,s_level) values(?,?,?) ";
					
		//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (TopicUserTags topicUsersetsUser : topicUserTags) {
					Object[] params = new Object[3];
					params[0] = topicUsersetsUser.getUser_id();
					params[1] = topicUsersetsUser.getUser_tag();
					params[2] = topicUsersetsUser.getS_level();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}

