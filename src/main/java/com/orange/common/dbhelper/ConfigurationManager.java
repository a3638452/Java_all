package com.orange.common.dbhelper;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 */
public class ConfigurationManager {
	
	// Properties对象使用private来修饰，就代表了其是类私有的
	// 那么外界的代码，就不能直接通过ConfigurationManager.prop这种方式获取到Properties对象
	// 之所以这么做，是为了避免外界的代码不小心错误的更新了Properties中某个key对应的value
	// 从而导致整个程序的状态错误，乃至崩溃
	private static Properties prop = new Properties();
	
	/**
	 * 静态代码块
	 * 
	 */
	static {
		try {
			
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("conf.properties"); 
		
			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	
	/**
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
	
}
