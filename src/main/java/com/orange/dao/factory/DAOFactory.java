package com.orange.dao.factory;

import com.orange.dao.AndroidPageSplitConvertRateDAO;
import com.orange.dao.IosPageSplitConvertRateDAO;
import com.orange.dao.UpdateUserLifeCycleDAO;
import com.orange.dao.UserBaseTagsDAO;
import com.orange.dao.UserLifeCycleDAO;
import com.orange.dao.UserModuleUsetimeDAO;
import com.orange.dao.realTimeDAO;
import com.orange.dao.userAactiveFrequencyDAO;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.ArticleUserSetsDAOImpl;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.UpdateUserLifeCycleImpl;
import com.orange.dao.impl.UpdateUserTagsImpl;
import com.orange.dao.impl.UserAactiveFrequencyImpl;
import com.orange.dao.impl.UserBaseTagsImpl;
import com.orange.dao.impl.UserLifeCycleImpl;
import com.orange.dao.impl.UserModuleUsetimeImpl;
import com.orange.dao.impl.UserTagsImpl;
import com.orange.dao.impl.UsersetsUserImpl;
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
	public  AndroidPageSplitConvertRateDAO getAndroidPageSplitConvertRateDAO1Day() {
		return new AndroidPageSplitConvertRateDAOImpl1Day();
	}
	/**
	
	 * 安卓页面7天的dao层结构
	 * @return
	 */
	public  AndroidPageSplitConvertRateDAO getAndroidPageSplitConvertRateDAO7Day() {
		return  new AndroidPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * 安卓页面14天的dao层结构
	 * @return
	 */
	public  AndroidPageSplitConvertRateDAO getAndroidPageSplitConvertRateDAO14Day() {
		return  new AndroidPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * 安卓页面30天的dao层结构
	 * @return
	 */
	public AndroidPageSplitConvertRateDAO getAndroidPageSplitConvertRateDAO30Day(){
		return new AndroidPageSplitConvertRateDAOImpl30Day();
	}
	
	/**
	 * ios页面1天的dao层结构
	 */
	public IosPageSplitConvertRateDAO getIosPageSplitConvertRateDAO1Day(){
		return new IosPageSplitConvertRateDAOImpl1Day();
	}
	
	/**
	 * ios页面7天的dao层结构
	 */
	public IosPageSplitConvertRateDAO getIosPageSplitConvertRateDAO07Day(){
		return new IosPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * ios页面14天的dao层结构
	 */
		public IosPageSplitConvertRateDAO getIosPageSplitConvertRateDAO14Day(){
		return new IosPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * ios页面30天的dao层结构
	 */
	public IosPageSplitConvertRateDAO getIosPageSplitConvertRateDAO30Day(){
		return new IosPageSplitConvertRateDAOImpl30Day();
	}
	
	/**
	 * 实时longindata数据持久化到mysql
	 */
	public static realTimeDAO getRealTimeLoginData(){
		return new realTimeDAOImpl();
		
	}
	
	/**
	 *  article,用户集合数据持久化到mysql
	 */
	public static ArticleUserSetsDAOImpl getTUserSets(){
		return new ArticleUserSetsDAOImpl();
		
	}
	
	/**
	 *  用户集合,用户数据持久化到mysql
	 */
	public static UsersetsUserImpl getTUser(){
		return new UsersetsUserImpl();
		
	}
	
	/**
	 *  执行插入用户画像mysql
	 */
	public static UserTagsImpl getUserTags(){
		return new UserTagsImpl();
		
	}
	
	/**
	 *  执行更新用户画像mysql
	 */
	public static UpdateUserTagsImpl updateUserTags(){
		return new UpdateUserTagsImpl();
	}
	
	

	/**
	 *  用户生命周期表执行插入mysql
	 */
	public static UserLifeCycleDAO getUserLifeCycleDAO() {
		return new UserLifeCycleImpl();
	}
	
	/**
	 *  更新用户生命周期表
	 */
	public static UpdateUserLifeCycleDAO getUpdateUserLifeCycleDAO() {
		return new UpdateUserLifeCycleImpl();
	}
	/**
	 *  用户模块使用时长表执行插入mysql
	 */
	public static UserModuleUsetimeDAO getUserModuleUsetimeDAO() {
		return new UserModuleUsetimeImpl();
	}
	
	/**
	 *  用户活躍頻數表C插入mysql
	 */
	public static userAactiveFrequencyDAO getuserAactiveFrequencyDAO() {
		return new UserAactiveFrequencyImpl();
	}
	
	/**
	 *  用户基本信息画像
	 */
	public static UserBaseTagsDAO getUserBaseTagsDAO() {
		return new UserBaseTagsImpl();
	}
	
	
	
	}
	
