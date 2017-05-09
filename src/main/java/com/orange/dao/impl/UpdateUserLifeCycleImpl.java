package com.orange.dao.impl;

import com.orange.bean.UserLifeCycle;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UpdateUserLifeCycleDAO;


public class UpdateUserLifeCycleImpl implements UpdateUserLifeCycleDAO{

	public void update(UserLifeCycle userLifeCycle) {      //where usr_id = (?) and user_tag = (?)
		//执行操作数据库的sql   ,use_time=use_time + 7
		String sql = "UPDATE t_report_user_login_usetime SET s_login_count =s_login_count+ ?,s_use_time =s_use_time+?,s_report_date = ? WHERE f_user_id = ? "; 
		//实例化数据结构数组
		Object[] params = new Object[]{
				userLifeCycle.getS_login_count(),
				userLifeCycle.getS_use_time(),
				userLifeCycle.getF_user_id(),
				userLifeCycle.getS_register_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	
}
