package com.orange.dao.impl;

import com.orange.bean.UserTags;
import com.orange.common.dbhelper.JDBCHelper;
import com.orange.dao.UpdateUserTagDAO;

public class UpdateUserTagsImpl  implements UpdateUserTagDAO{

	


		@Override
		public void update(UserTags userTags) {//where usr_id = (?) and user_tag = (?)
			//执行操作数据库的sql
			String sql = "update  t_user_tags set s_level=s_level + 1,create_time=? where user_id = ? AND user_tag =? "; 
			//实例化数据结构数组
			Object[] params = new Object[]{
					userTags.getCreate_time(),
					userTags.getUser_id(),
					userTags.getUser_tag()
			};
			
			JDBCHelper jdbcHelper = JDBCHelper.getInstance();
			jdbcHelper.executeUpdate(sql, params);
		}

	}


