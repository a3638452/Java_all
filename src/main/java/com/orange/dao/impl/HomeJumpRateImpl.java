package com.orange.dao.impl;

import com.orange.bean.HomeJumpRate;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.HomeJumpRateDAO;

public class HomeJumpRateImpl implements HomeJumpRateDAO{

	@Override
	public void insert(HomeJumpRate homeJumpRate) {

		String sql = "insert into t_exue_jump_rate(report_date,exue_jump_rate,android_jump_rate,ios_jump_rate) values(?,?,?,?)";  
		
		Object[] params = new Object[]{
				homeJumpRate.getReport_date(),
				homeJumpRate.getExue_jump_rate(),
				homeJumpRate.getAndroid_jump_rate(),
				homeJumpRate.getIos_jump_rate()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
