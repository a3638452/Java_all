package com.orange.dao.impl;


import java.util.ArrayList;
import java.util.List;

import com.orange.bean.ModuleUserCountBean;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.Update2DayModuleUserCountDAO;

public class Update2DayModuleUserCountImpl implements Update2DayModuleUserCountDAO{

	@Override
	public void updateBatch(List<ModuleUserCountBean> moduleUserCountBean) {
				
				// 对于需要更新的数据，执行批量更新操作
				String updateSQL = "UPDATE t_report_module_retention "
						+ "SET next_day=? "
						+ "WHERE module=? "
						+ "AND s_report_date=? ";
				
				List<Object[]> updateParamsList = new ArrayList<Object[]>();
				for(ModuleUserCountBean bean : moduleUserCountBean) {
					Object[] params = new Object[]{
							bean.getNext_day(),
							bean.getModule(),
							bean.getS_report_date()
							};
					updateParamsList.add(params);
				}
				JDBCHelper jdbcHelper = JDBCHelper.getInstance();
				jdbcHelper.executeBatch(updateSQL, updateParamsList);
	}
	
	@Override
	public void updateBatch3(List<ModuleUserCountBean> moduleUserCountBean) {
		
		// 对于需要更新的数据，执行批量更新操作
		String updateSQL = "UPDATE t_report_module_retention "
				+ "SET three_day=? "
				+ "WHERE module=? "
				+ "AND s_report_date=? ";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		for(ModuleUserCountBean bean : moduleUserCountBean) {
			Object[] params = new Object[]{
					bean.getThree_day(),
					bean.getModule(),
					bean.getS_report_date()
					};
			updateParamsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
}
	
	@Override
	public void updateBatch7(List<ModuleUserCountBean> moduleUserCountBean) {
		
		// 对于需要更新的数据，执行批量更新操作
		String updateSQL = "UPDATE t_report_module_retention "
				+ "SET seven_day=? "
				+ "WHERE module=? "
				+ "AND s_report_date=? ";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		for(ModuleUserCountBean bean : moduleUserCountBean) {
			Object[] params = new Object[]{
					bean.getSeven_day(),
					bean.getModule(),
					bean.getS_report_date()
					};
			updateParamsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
}
	
	@Override
	public void updateBatch14(List<ModuleUserCountBean> moduleUserCountBean) {
		
		// 对于需要更新的数据，执行批量更新操作
		String updateSQL = "UPDATE t_report_module_retention "
				+ "SET fourteen_day=? "
				+ "WHERE module=? "
				+ "AND s_report_date=? ";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		for(ModuleUserCountBean bean : moduleUserCountBean) {
			Object[] params = new Object[]{
					bean.getFourteen_day(),
					bean.getModule(),
					bean.getS_report_date()
					};
			updateParamsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
}
	
	@Override
	public void updateBatch30(List<ModuleUserCountBean> moduleUserCountBean) {
		
		// 对于需要更新的数据，执行批量更新操作
		String updateSQL = "UPDATE t_report_module_retention "
				+ "SET thirty_day=? "
				+ "WHERE module=? "
				+ "AND s_report_date=? ";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		for(ModuleUserCountBean bean : moduleUserCountBean) {
			Object[] params = new Object[]{
					bean.getThirty_day(),
					bean.getModule(),
					bean.getS_report_date()
					};
			updateParamsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
}
	
}
