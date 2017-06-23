package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.TopicRecommendSets;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.TopicRecommendSetsDAO;



public class TopicRecommendSetsDAOImpl implements TopicRecommendSetsDAO {

	@Override
	public void insert(TopicRecommendSets topicRecommendSets) {
		//执行操作数据库的sql
		String sql = "insert into t_user_topic_recommend(topic_id,userset_id,opt_type,article_time) values(?,?,?,?)"; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				topicRecommendSets.getTopic_id(),
				topicRecommendSets.getUserset_id(),
				topicRecommendSets.getOpt_type(),
				topicRecommendSets.getArticle_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
	@Override
	public void insertBatch(List<TopicRecommendSets> topicRecommendSets) {

		/**
		 * 批量插入session明细数据
		 * @param sessionDetails
		 */
		//执行操作数据库的sql
		String sql = "insert into t_user_topic_recommend(topic_id,userset_id,opt_type,article_time) values(?,?,?,?)"; 
					
					//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (TopicRecommendSets topicRecommendSet : topicRecommendSets) {
							Object[] params = new Object[4];
							params[0] = topicRecommendSet.getTopic_id();
							params[1] = topicRecommendSet.getUserset_id();
							params[2] = topicRecommendSet.getOpt_type();
							params[3] = topicRecommendSet.getArticle_time();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}

