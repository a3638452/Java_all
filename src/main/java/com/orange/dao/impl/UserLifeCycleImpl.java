package com.orange.dao.impl;

import com.orange.bean.UserLifeCycle;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserLifeCycleDAO;


public class UserLifeCycleImpl implements UserLifeCycleDAO {
	public void insert(UserLifeCycle userLifeCycle) {
		String sql = "insert into t_report_user_login_usetime(f_user_id,s_xiaoxincode,s_account,s_login_count,s_use_time,s_register_time,s_create_time,s_report_date,s_update_time) values(?,?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userLifeCycle.getF_user_id(),
				userLifeCycle.getS_xiaoxincode(),
				userLifeCycle.getS_account(),
				userLifeCycle.getS_login_count(),
				userLifeCycle.getS_use_time(),
				userLifeCycle.getS_register_time(),
				userLifeCycle.getS_create_time(),
				userLifeCycle.getS_report_date(),
				userLifeCycle.getS_update_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
