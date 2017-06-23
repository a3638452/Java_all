package com.orange.dao;

import java.util.List;

import com.orange.bean.UserTags;




/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface UserTagsDAO {

	void insert(UserTags userTags);
	
	/**
	 * 批量插入
	 * @param 
	 */
	void insertBatch(List<UserTags> userTags);
	
}
