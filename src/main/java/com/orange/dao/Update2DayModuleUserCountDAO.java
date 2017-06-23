package com.orange.dao;


import java.util.List;

import com.orange.bean.ModuleUserCountBean;

public interface Update2DayModuleUserCountDAO {

	//执行批量更新
	void updateBatch(List<ModuleUserCountBean> moduleUserCountBean);
	void updateBatch3(List<ModuleUserCountBean> moduleUserCountBean);
	void updateBatch7(List<ModuleUserCountBean> moduleUserCountBean);
	void updateBatch14(List<ModuleUserCountBean> moduleUserCountBean);
	void updateBatch30(List<ModuleUserCountBean> moduleUserCountBean);
}
