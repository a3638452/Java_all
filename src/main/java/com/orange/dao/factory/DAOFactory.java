package com.orange.dao.factory;

import com.orange.common.dbhelper.DaoFactoryUtil;
import com.orange.dao.AndroidPageSplitConvertRateDAO;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.realTimeDAOImpl;



/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {
	


	/**
	 * 安卓页面1天的dao层结构
	 * @return
	 */
	public  AndroidPageSplitConvertRateDAOImpl1Day getAndroidPageSplitConvertRateDAO1Day() {
		return (AndroidPageSplitConvertRateDAOImpl1Day) new DaoFactoryUtil().getDaoFactory(AndroidPageSplitConvertRateDAOImpl1Day.class);
	}
	/**
	
	 * 安卓页面7天的dao层结构
	 * @return
	 */
	public  AndroidPageSplitConvertRateDAOImpl7Day getAndroidPageSplitConvertRateDAO7Day() {
		return (AndroidPageSplitConvertRateDAOImpl7Day) new DaoFactoryUtil().getDaoFactory(AndroidPageSplitConvertRateDAOImpl7Day.class);
	}
	
	/**
	 * 安卓页面14天的dao层结构
	 * @return
	 */
	public  AndroidPageSplitConvertRateDAOImpl14Day getAndroidPageSplitConvertRateDAO14Day() {
		return (AndroidPageSplitConvertRateDAOImpl14Day) new DaoFactoryUtil().getDaoFactory(AndroidPageSplitConvertRateDAOImpl14Day.class);
	}
	
	/**
	 * 安卓页面30天的dao层结构
	 * @return
	 */
	public AndroidPageSplitConvertRateDAOImpl30Day getAndroidPageSplitConvertRateDAO30Day(){
		return (AndroidPageSplitConvertRateDAOImpl30Day) new DaoFactoryUtil().getDaoFactory(AndroidPageSplitConvertRateDAOImpl30Day.class);
	}
	
	/**
	 * ios页面1天的dao层结构
	 */
	public IosPageSplitConvertRateDAOImpl1Day getIosPageSplitConvertRateDAO1Day(){
		return (IosPageSplitConvertRateDAOImpl1Day) new DaoFactoryUtil().getDaoFactory(IosPageSplitConvertRateDAOImpl1Day.class);
	}
	
	/**
	 * ios页面7天的dao层结构
	 */
	public IosPageSplitConvertRateDAOImpl7Day getIosPageSplitConvertRateDAO07Day(){
		return (IosPageSplitConvertRateDAOImpl7Day) new DaoFactoryUtil().getDaoFactory(IosPageSplitConvertRateDAOImpl7Day.class);
	}
	
	/**
	 * ios页面14天的dao层结构
	 */
		public IosPageSplitConvertRateDAOImpl14Day getIosPageSplitConvertRateDAO14Day(){
		return (IosPageSplitConvertRateDAOImpl14Day) new DaoFactoryUtil().getDaoFactory(IosPageSplitConvertRateDAOImpl14Day.class);
	}
	
	/**
	 * ios页面30天的dao层结构
	 */
	public IosPageSplitConvertRateDAOImpl30Day getIosPageSplitConvertRateDAO30Day(){
		return (IosPageSplitConvertRateDAOImpl30Day) new DaoFactoryUtil().getDaoFactory(IosPageSplitConvertRateDAOImpl30Day.class);
	}
	
	/**
	 * 实时longindata数据持久化到mysql
	 */
	public static realTimeDAOImpl getRealTimeLoginData(){
		return new realTimeDAOImpl();
		
	}
	
	}
	
