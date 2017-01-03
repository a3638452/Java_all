package com.orange.common.dbhelper;

import java.io.IOException;
import java.util.Properties;

import com.orange.common.util.Constants;

/**
 * 获取到配置文件信息
 * @author Administrator
 *
 */
public class PropertiesUtil {

//	public static Properties get_jdbconnect(){
//		Properties prop = new Properties();
//		prop.put("user",Constants.JDBC_USER);
//		prop.put("password",Constants.JDBC_PASSWORD);
//		return prop;
//	}
//	
//	
	public static Properties getProperties(){
		Properties defProps = new Properties();
		try {
			defProps.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("conf.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return defProps;
	}
}
