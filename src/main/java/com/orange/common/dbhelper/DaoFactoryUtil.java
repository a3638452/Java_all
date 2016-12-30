package com.orange.common.dbhelper;

/**
 * daoFactory 动态的工厂方法模式
 * 1、进行类反射
 * 2、实例化
 * @author Administrator
 *
 */
public class DaoFactoryUtil {
	public Object getDaoFactory(Class<?> clz){
		Object obj = null;
		try {
			obj = Class.forName(clz.getName()).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return obj;
	}

}
