package com.orange.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.orange.bean.LogQuestions;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.LogQuestionsDAO;


public class LogQuestionsDAOImpl implements LogQuestionsDAO{

	
	@Override
	public void insert(LogQuestions logQuestions) {
		String sql = "insert into t_log_que(user_id,que_id,que_time) values(?,?,?)";
		
		 Object[] params = new Object[]{
				 logQuestions.getUser_id(),
				 logQuestions.getQue_id(),
				 logQuestions.getQue_time()
		 };
		 
		 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		 jdbcHelper.executeUpdate(sql, params);
		
	}
	
	@Override
	public void insertBatch(List<LogQuestions> logQuestions) {

		//执行操作数据库的sql
		String sql = "insert into t_log_que(user_id,que_id,que_time) values(?,?,?)";
					
		//实例化数据结构数组
		List<Object[]> paramsList = new ArrayList<Object[]>();
				for (LogQuestions logQuestion : logQuestions) {
					Object[] params = new Object[3];
					params[0] = logQuestion.getUser_id();
					params[1] = logQuestion.getQue_id();
					params[2] = logQuestion.getQue_time();
							
					paramsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();			
				jdbcHelper.executeBatch(sql, paramsList);		
				}
}

