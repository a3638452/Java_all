package com.orange.dao.impl;

import com.orange.bean.UserTags;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserTagsDAO;

public class UserTagsImpl implements UserTagsDAO {

	@Override
	public void insert(UserTags userTags) {
		//执行操作数据库的sql
		String sql = " insert into t_user_tags values(?,?,?,?,?,?) "; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				
				userTags.getP_id(),
				userTags.getUser_id(),
				userTags.getUser_tag(),
				userTags.getS_level(),
				userTags.getCreate_time(),
				userTags.getUpdate_time()
				
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}

