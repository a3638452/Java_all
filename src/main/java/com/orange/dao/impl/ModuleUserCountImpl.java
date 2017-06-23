package com.orange.dao.impl;


import com.orange.bean.ModuleUserCountBean;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.ModuleUserCountDAO;

public class ModuleUserCountImpl implements ModuleUserCountDAO{

	@Override
	public void insert(ModuleUserCountBean moduleUserCountBean) {
		String sql = "insert into t_report_module_retention(s_report_date,module,user_count) values(?,?,?)";  
		
			Object[] params = new Object[]{
			moduleUserCountBean.getS_report_date(),
			moduleUserCountBean.getModule(),
			moduleUserCountBean.getUser_count()
		};
			
			JDBCHelper jdbcHelper = JDBCHelper.getInstance();
			jdbcHelper.executeUpdate(sql, params);
			
		}
		
	}
		
