package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.UserTags;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserTagsDAO;



public class UserTagsImpl implements UserTagsDAO {

	@Override
	public void insert(UserTags userTags) {
		//执行操作数据库的sql
		String sql = " insert into t_user_tags(user_id,user_tag,s_level) values(?,?,?) ";
		
		//实例化数据结构数组
		Object[] params = new Object[]{
				
				userTags.getUser_id(),
				userTags.getUser_tag(),
				userTags.getS_level()
				
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	/**
	 * 批量插入session明细数据
	 * @param sessionDetails
	 */
	@Override
	public void insertBatch(List<UserTags> userTags) {
		//执行操作数据库的sql
			String sql = " insert into t_user_tags(user_id,user_tag,s_level) values(?,?,?) ";
				
				//实例化数据结构数组
			ArrayList<Object[]> paramsList = new ArrayList<>();
			for (UserTags userTag : userTags) {
				Object[] params = new Object[3];
				params[0] = userTag.getUser_id();
				params[1] = userTag.getUser_tag();
				params[2] = userTag.getS_level();
				paramsList.add(params);
				
			}
			JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
			jdbcHelper.executeBatch(sql, paramsList);		
			}
		
		
	}

