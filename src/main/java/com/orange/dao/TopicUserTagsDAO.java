package com.orange.dao;

import java.util.List;

import com.orange.bean.TopicUserTags;



/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface TopicUserTagsDAO {

	void insert(TopicUserTags topicUserTags);
	
	void insertBatch(List<TopicUserTags> topicUserTags);
	
}
