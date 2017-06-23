package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.ArticleUserSets;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.ArticleUserSetsDAO;



public class ArticleUserSetsDAOImpl implements ArticleUserSetsDAO {

	@Override
	public void insert(ArticleUserSets artUserSets) {
		//执行操作数据库的sql
		String sql = "insert into t_user_recommend(article_id,userset_id,recommend_type,opt_type,article_time) values(?,?,?,?,?)"; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				artUserSets.getArticle_id(),
				artUserSets.getUserset_id(),
				artUserSets.getRecommend_type(),
				artUserSets.getOpt_type(),
				artUserSets.getArticle_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	@Override
	public void insertBatch(List<ArticleUserSets> articleUserSets) {

		/**
		 * 批量插入session明细数据
		 * @param sessionDetails
		 */
			//执行操作数据库的sql
		String sql = "insert into t_user_recommend(article_id,userset_id,recommend_type,opt_type,article_time) values(?,?,?,?,?)";
					
					//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (ArticleUserSets articleUserSet : articleUserSets) {
					Object[] params = new Object[5];
							params[0] = articleUserSet.getArticle_id();
							params[1] = articleUserSet.getUserset_id();
							params[2] = articleUserSet.getRecommend_type();
							params[3] = articleUserSet.getOpt_type();
							params[4] = articleUserSet.getArticle_time();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}

