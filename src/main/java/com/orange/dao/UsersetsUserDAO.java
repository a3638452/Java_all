package com.orange.dao;

import java.util.List;

import com.orange.bean.UsersetsUser;




/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface UsersetsUserDAO {

	void insert(UsersetsUser usersetsUser);

	/**
	 * 批量插入
	 * @param 
	 */
	void insertBatch(List<UsersetsUser> usersetsUser);
}
