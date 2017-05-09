package com.orange.dao.impl;

import com.orange.bean.UserModuleUsetime;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserModuleUsetimeDAO;


public class UserModuleUsetimeImpl implements  UserModuleUsetimeDAO{

	@Override
	public void insert(UserModuleUsetime userModuleUsetime) {

String sql = "insert into t_repot_user_modules_usetime(f_user_id,s_user_name,s_xiaoxincode,s_module_name,s_use_time,s_report_date) values(?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userModuleUsetime.getF_user_id(),
				userModuleUsetime.getS_user_name(),
				userModuleUsetime.getS_xiaoxincode(),
				userModuleUsetime.getS_module_name(),
				userModuleUsetime.getS_use_time(),
				userModuleUsetime.getS_report_date()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

	
}
