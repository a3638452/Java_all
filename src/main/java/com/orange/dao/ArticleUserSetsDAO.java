package com.orange.dao;

import java.util.List;

import com.orange.bean.ArticleUserSets;




/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface ArticleUserSetsDAO {

	void insert(ArticleUserSets articleUserSets);
	
	void insertBatch(List<ArticleUserSets> articleUserSets);
	
}
