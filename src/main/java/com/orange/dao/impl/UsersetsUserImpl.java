package com.orange.dao.impl;

import com.orange.bean.UsersetsUser;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UsersetsUserDAO;

public class UsersetsUserImpl implements UsersetsUserDAO {

	@Override
	public void insert(UsersetsUser usersetsUser) {
		//执行操作数据库的sql
		String sql = "insert into t_user_list(userset_id,user_id) values(?,?) "; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				usersetsUser.getUserset_id(),
				usersetsUser.getUser_id()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}

