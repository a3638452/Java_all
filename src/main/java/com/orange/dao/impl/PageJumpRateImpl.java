package com.orange.dao.impl;

import com.orange.bean.PageJumpRateBean;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.PageJumpRateDAO;

public class PageJumpRateImpl implements PageJumpRateDAO{

	@Override
	public void insert(PageJumpRateBean pageJumpRate) {

		String sql = "insert into t_page_jump(pagename,jump_rate,report_date) values(?,?,?)";  
		
		Object[] params = new Object[]{
				pageJumpRate.getPagename(),
				pageJumpRate.getJump_rate(),
				pageJumpRate.getReport_date()
	};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

}
