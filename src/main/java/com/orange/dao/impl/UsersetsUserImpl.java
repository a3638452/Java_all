package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

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

	
	/**
	 * 批量插入
	 */
	@Override
	public void insertBatch(List<UsersetsUser> usersetsUser) {
			//执行操作数据库的sql
		String sql = "insert into t_user_list(userset_id,user_id) values(?,?) "; 
					
					//实例化数据结构数组
		     List<Object[]> paramsList = new ArrayList<Object[]>();
				for (UsersetsUser usersets : usersetsUser) {
					Object[] params = new Object[2];
						 params[0] = usersets.getUserset_id();
						 params[1] = usersets.getUser_id();
						 paramsList.add(params);
					};
					
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);	
	}
}

