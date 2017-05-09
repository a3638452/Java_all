package com.orange.dao.impl;

import com.orange.bean.UserAactiveFrequency;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.userAactiveFrequencyDAO;

public class UserAactiveFrequencyImpl implements userAactiveFrequencyDAO{

	public void insert(UserAactiveFrequency userAactiveFrequency) {

String sql = "insert into t_report_user_active_frequency(f_province_id,f_city_id,f_area_id,s_loyal_user,s_continue_active,s_backflow,s_recent_lost,s_report_date) values(?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userAactiveFrequency.getF_province_id(),
				userAactiveFrequency.getF_city_id(),
				userAactiveFrequency.getF_area_id(),
				userAactiveFrequency.getS_loyal_user(),
				userAactiveFrequency.getS_continue_active(),
				userAactiveFrequency.getS_backflow(),
				userAactiveFrequency.getS_recent_lost(),
				userAactiveFrequency.getS_report_date()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

	
}