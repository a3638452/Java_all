package com.orange.dao.impl;

import com.orange.bean.UserLifeCycle;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserLifeCycleDAO;




public class UserLifeCycleImpl implements UserLifeCycleDAO {
	public void insert(UserLifeCycle userLifeCycle) {
		String sql = "insert into t_user_life_cycle values(?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userLifeCycle.getF_user_id(),
				userLifeCycle.getS_xiaoxincode(),
				userLifeCycle.getS_account(),
				userLifeCycle.getS_login_count(),
				userLifeCycle.getS_use_time(),
				userLifeCycle.getS_register_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}
