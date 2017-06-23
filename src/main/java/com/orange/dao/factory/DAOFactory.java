package com.orange.dao.factory;

import com.orange.dao.ArticleUserSetsDAO;
import com.orange.dao.DayUseTimeDAO;
import com.orange.dao.HomeJumpRateDAO;
import com.orange.dao.LogQuestionsDAO;
import com.orange.dao.LogTopicsDAO;
import com.orange.dao.ModuleUserCountDAO;
import com.orange.dao.PageJumpRateDAO;
import com.orange.dao.TopicRecommendSetsDAO;
import com.orange.dao.TopicUserTagsDAO;
import com.orange.dao.TopicUsersetsUserDAO;
import com.orange.dao.Update2DayModuleUserCountDAO;
import com.orange.dao.UserBaseTagsDAO;
import com.orange.dao.UserLifeCycleDAO;
import com.orange.dao.UserTagsDAO;
import com.orange.dao.UsersetsUserDAO;
import com.orange.dao.realTimeDAO;
import com.orange.dao.userAactiveFrequencyDAO;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.AndroidPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.ArticleUserSetsDAOImpl;
import com.orange.dao.impl.DayUseTimeImpl;
import com.orange.dao.impl.HomeJumpRateImpl;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl14Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl1Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl30Day;
import com.orange.dao.impl.IosPageSplitConvertRateDAOImpl7Day;
import com.orange.dao.impl.LogQuestionsDAOImpl;
import com.orange.dao.impl.LogTopicsDAOImpl;
import com.orange.dao.impl.ModuleUserCountImpl;
import com.orange.dao.impl.PageJumpRateImpl;
import com.orange.dao.impl.TopicRecommendSetsDAOImpl;
import com.orange.dao.impl.TopicUserTagsImpl;
import com.orange.dao.impl.TopicUsersetsUserImpl;
import com.orange.dao.impl.Update2DayModuleUserCountImpl;
import com.orange.dao.impl.UserAactiveFrequencyImpl;
import com.orange.dao.impl.UserBaseTagsImpl;
import com.orange.dao.impl.UserLifeCycleImpl;
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
	public static AndroidPageSplitConvertRateDAOImpl1Day getAndroidPageSplitConvertRateDAO1Day() {
		return new AndroidPageSplitConvertRateDAOImpl1Day();
	}
	/**
	
	 * 安卓页面7天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl7Day getAndroidPageSplitConvertRateDAO7Day() {
		return new AndroidPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * 安卓页面14天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl14Day getAndroidPageSplitConvertRateDAO14Day() {
		return new AndroidPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * 安卓页面30天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl30Day getAndroidPageSplitConvertRateDAO30Day() {
		return new AndroidPageSplitConvertRateDAOImpl30Day();
	}
	
	/**
	 * ios页面1天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl1Day getIosPageSplitConvertRateDAO1Day() {
		return new IosPageSplitConvertRateDAOImpl1Day();
	}
	
	/**
	 * ios页面7天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl7Day getIosPageSplitConvertRateDAO7Day() {
		return new IosPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * ios页面14天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl14Day getIosPageSplitConvertRateDAO14Day() {
		return new IosPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * ios页面30天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl30Day getIosPageSplitConvertRateDAO30Day() {
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
	public static ArticleUserSetsDAO getTUserSets(){
		return new ArticleUserSetsDAOImpl();
	}
	
	/**
	 *  用户集合,用户数据持久化到mysql
	 */
	public static UsersetsUserDAO getTUser(){
		return new UsersetsUserImpl();
	}
	
	/**
	 *  执行插入用户画像mysql
	 */
	public static  UserTagsDAO getUserTags(){
		return new UserTagsImpl();
	}
	
	/**
	 *  用户生命周期指标执行插入mysql
	 */
	public static UserLifeCycleDAO getUserLifeCycleDAO() {
		return new UserLifeCycleImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicUserTagsDAO getTopicUserTagsDAO(){
		return new TopicUserTagsImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicRecommendSetsDAO getTopicRecommendSetsDAO(){
		return new TopicRecommendSetsDAOImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicUsersetsUserDAO getTopicUsersetsUserDAO(){
		return new TopicUsersetsUserImpl();
	}
	
	/**
	 *  清洗日志topic数据持久化到mysql
	 */
	public static LogTopicsDAO getLogTopicsDAO(){
		return new LogTopicsDAOImpl();
	}
	
	/**
	 *  清洗日志topic数据持久化到mysql
	 */
	public static LogQuestionsDAO getLogQuestionsDAO(){
		return new LogQuestionsDAOImpl();
	}
	
	/**
	 *  用户基本信息画像
	 */
	public static UserBaseTagsDAO getUserBaseTagsDAO() {
		return new UserBaseTagsImpl();
	}
	
	/**
	 *  插入昨天新注册用户模块用户量
	 */
	public static ModuleUserCountDAO getModuleUserCountDAO() {
		return new ModuleUserCountImpl();
	}
	
	/**
	 *  执行更新2天前的留存用户
	 */
	public static Update2DayModuleUserCountDAO getUpdate2DayModuleUserCountDAO() {
		return new Update2DayModuleUserCountImpl();
	}
	
	/**
	 *  用户停留时长统计执行插入mysql
	 */
	public static DayUseTimeDAO getDayUseTimeDAO() {
		return new DayUseTimeImpl();
	}
	
	/**
	 *  e学产品跳出率执行插入mysql
	 */
	public static HomeJumpRateDAO getHomeJumpRateDAO() {
		return new HomeJumpRateImpl();
	}
	
	
	/**
	 *  页面跳出率执行插入mysql
	 */
	public static PageJumpRateDAO getPageJumpRateDAO() {
		return new PageJumpRateImpl();
	}
	
	/**
	 *  用户活躍頻數表C插入mysql
	 */
	public static userAactiveFrequencyDAO getuserAactiveFrequencyDAO() {
		return new UserAactiveFrequencyImpl();
	}
	
	}
	
