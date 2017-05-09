package com.orange.dao.impl;

import com.orange.bean.UserBaseTags;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UserBaseTagsDAO;

public class UserBaseTagsImpl implements UserBaseTagsDAO{

	public void insert(UserBaseTags userBaseTags) {

String sql = "insert into t_user_base_tags(s_user_id,s_user_type,f_province_id,f_city_id,f_area_id,s_school_type,s_report_date) values(?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userBaseTags.getS_user_id(),
				userBaseTags.getS_user_type(),
				userBaseTags.getF_province_id(),
				userBaseTags.getF_city_id(),
				userBaseTags.getF_area_id(),
				userBaseTags.getS_school_type(),
				userBaseTags.getS_report_date()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

}
