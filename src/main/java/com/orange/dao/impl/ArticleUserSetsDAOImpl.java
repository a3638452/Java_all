package com.orange.dao.impl;

import com.orange.bean.ArticleUserSets;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.ArticleUserSetsDAO;

public class ArticleUserSetsDAOImpl implements ArticleUserSetsDAO {

	@Override
	public void insert(ArticleUserSets artUserSets) {
		//执行操作数据库的sql
		String sql = "insert into t_user_recommend values(?,?,?,?,?,?)"; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				artUserSets.getP_id(),
				artUserSets.getArticle_id(),
				artUserSets.getUserset_id(),
				artUserSets.getRecommend_type(),
				artUserSets.getArticle_time(),
				artUserSets.getCreate_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}

