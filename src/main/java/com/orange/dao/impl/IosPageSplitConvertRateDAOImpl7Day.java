package com.orange.dao.impl;

import com.orange.bean.PageSplitConvertRate;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.IosPageSplitConvertRateDAO;



/**
 * 页面切片转化率DAO实现类
 * @author Administrator
 *
 */
public class IosPageSplitConvertRateDAOImpl7Day implements IosPageSplitConvertRateDAO {

	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_convert_rate_ios_7d values(?,?,?,?,?,?)";  
		Object[] params = new Object[]{
				 pageSplitConvertRate.getId()
				,pageSplitConvertRate.getPage_split()
				,pageSplitConvertRate.getStart_convert_rate()
				,pageSplitConvertRate.getLast_convert_rate()
				,pageSplitConvertRate.getPv()
				,pageSplitConvertRate.getCreate_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
