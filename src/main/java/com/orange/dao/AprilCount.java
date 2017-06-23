package com.orange.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.common.dbhelper.PropertiesUtil;
import com.orange.common.dbhelper.ConfigurationManager;
import com.orange.common.util.Constants;

public class AprilCount implements Serializable {

	//学校ID、班级ID、班级名称、总学生人数、总用户数（家长+老师）、总注册量（家长+老师数）、报表日期、创建时间
	public void classUser(SparkSession session) {

		// 读取数据表
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_user_base",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_user_base");
		session.read()
				.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_class", PropertiesUtil.getProperties())
				.toDF().createOrReplaceTempView("t_class");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_class_teacher_map",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_class_teacher_map");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_student_class_map",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_student_class_map");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_student_parent_map",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_student_parent_map");

		// 读取
		session.sql("SELECT f_school_id,p_id,s_name FROM t_class where s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')").toDF().registerTempTable("dt_class");
		session.sql("SELECT f_class_id,f_user_id FROM t_class_teacher_map where s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')").toDF().registerTempTable("dt_teacher");
		session.sql("SELECT f_class_id,f_student_id FROM t_student_class_map where s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')").toDF().registerTempTable("dt_student");
		session.sql("SELECT b.f_class_id,a.f_par_user_id FROM t_student_parent_map a,t_student_class_map b WHERE a.s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') and b.s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') and a.f_student_id=b.f_student_id")
				.toDF().createOrReplaceTempView("dt_parent");
		session.sql("select p_id,s_is_registed from t_user_base").toDF().createOrReplaceTempView("dt_base");

		// session.sql("").toDF().registerTempTable("");
		session.sql("SELECT f_class_id class,f_user_id users,'tea' type from dt_teacher union "
				+ "SELECT f_class_id class,f_par_user_id users,'par' type from dt_parent").toDF()
				.createOrReplaceTempView("dt_user");

		// 班级ID、总学生人数
		session.sql("select f_class_id class,count(*) co from dt_student group by f_class_id").toDF()
				.createOrReplaceTempView("dt_count_student");

		// 班级ID、总老师家长数
		session.sql("select class,count(*) co from dt_user group by class").toDF().createOrReplaceTempView("dt_count_user");

		// 为用户匹配是否是注册用户
		session.sql("select class,users,s_is_registed from dt_user,dt_base where dt_user.users=dt_base.p_id").toDF()
				.createOrReplaceTempView("dt_registed_user");

		// 班级ID、总老师家长数 (总注册用户数)
		session.sql("select class,count(*) co from dt_registed_user where s_is_registed = '1' group by class").toDF()
				.createOrReplaceTempView("dt_count_registed_user");

		// 目标：学校ID、班级ID、班级名称、总学生人数、总用户数（家长+老师）、总注册量（家长+老师数）、报表日期、创建时间
		Dataset<Row> sqlDF = session.sql("select "
		+ "cl.f_school_id f_school_id,"
				+ "cl.p_id f_class_id,"
				+ "cl.s_name s_class_name,"
				+ "cs.co s_student_count,"
				+ "cu.co s_user_count,"
				+ "cru.co s_registed_user_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "from dt_class cl,dt_count_student cs,dt_count_user cu,dt_count_registed_user cru "
				+ "where cl.p_id=cs.class and cl.p_id=cu.class and cl.p_id=cru.class");

		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_class_user",
				PropertiesUtil.getProperties());
	}

	
	

	//目的：学校ID、学校名称、总绑卡量、有效打卡量、统计年月日、报表日期、创建时间
		public void schoolCard(SparkSession session) throws SQLException {
			// 读取数据表
			
			
			Properties propt = new Properties();
			propt.put("user","root");
			propt.put("password","test1pass");
			session.read()
					.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_school", PropertiesUtil.getProperties())
					.toDF().createOrReplaceTempView("t_school");
			session.read()
					.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_card", PropertiesUtil.getProperties())
					.toDF().createOrReplaceTempView("t_card");
			session.read()
					.jdbc("jdbc:mysql://192.168.0.16:3306/information_schema?characterEncoding=utf-8", "tables", propt)
					.toDF().createOrReplaceTempView("tables");

			// 目的：学校ID、学校名称、总绑卡量、有效打卡量、打卡日期、报表日期、创建时间

			// 表名列表
			JavaRDD<Row> javaRDD1 = session
					.sql("SELECT table_name FROM tables WHERE table_name LIKE 't_card_checkInfo_%-%' AND table_type='base table'")
					.toJavaRDD();
			JavaRDD<String> table_names = javaRDD1.map(new Function<Row, String>() {
				@Override
				public String call(Row row) throws Exception {
					return row.getString(0);
				}
			});

			// 总绑卡数
			List<Row> co_card = session.sql("SELECT f_school_id,COUNT(*) FROM t_card where s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') GROUP BY f_school_id").toDF()
					.collectAsList();

			
			
			Date day = new Date();
			SimpleDateFormat zero_time = new SimpleDateFormat("yyyy-MM-dd 00:00:00");// 设置日期格式
			String today = zero_time.format(day);
			day.setDate(day.getDate()-1);
			String yesterday = zero_time.format(day);

			// 有效考勤数
			List<String> co_kaoqin = new ArrayList<String>();
			for (String table_name : table_names.collect()) {
				session.read().jdbc("jdbc:mysql://192.168.0.16:3306/exiaoxincard?characterEncoding=utf-8", "`" + table_name + "`",
						propt).toDF().createOrReplaceTempView("t_school_card");

				session.read().jdbc("jdbc:mysql://192.168.0.16:3306/exiaoxincard?characterEncoding=utf-8", "`" + table_name + "`",
						new String[]{"s_time >= '"+ yesterday + "' and s_time <= '" + today + "'"}, propt).toDF().createOrReplaceTempView("t_school_card");
				String count_card = session.sql("SELECT COUNT(*) FROM t_school_card WHERE s_type=1").first().toString();
				co_kaoqin.add(table_name.substring(17, 53) + "," + count_card.subSequence(1, count_card.length() - 1));

			}

			// 学校信息表
			List<Row> co_school = session.sql("SELECT p_id,s_name FROM t_school").toDF().collectAsList();

			// 学校id，绑卡数，打卡数
			List<String> co_res1 = new ArrayList<String>();
			for (String str : co_kaoqin) {
				String[] kao = str.split(",");
				for (Row row : co_card) {
					String[] card = row.toString().substring(1, row.toString().length() - 1).split(",");
					if (kao[0].equals(card[0])) {
						co_res1.add(kao[0] + "," + card[1] + "," + kao[1]);
					}
				}
			}

			// 将结果进行拼
			List<String> co_res2 = new ArrayList<String>();
			for (String str : co_res1) {
				String[] res1 = str.split(",");
				for (Row row : co_school) {
					String[] sc = row.toString().substring(1, row.toString().length() - 1).split(",");
					if (res1[0].equals(sc[0])) {

						co_res2.add(sc[0] + "," + sc[1] + "," + res1[1] + "," + res1[2]);

					}
				}
			}

			// 插入数据库
//			String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
//			String user = "root";
//			String password = "test3pass";

			String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
			String user = "xxv2";
			String password = "xv2PassWD-321";
			

			String sql = "INSERT INTO t_report_school_card(f_school_id,s_school_name,s_card_count,s_clock_count,s_clock_date,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
			Connection connection = DriverManager.getConnection(url, user, password);
			PreparedStatement pst = connection.prepareStatement(sql);
			final int batchSize = 1000;
			int count = 0;
			Date now = new Date();
			SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
			SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式

			String create_time = time.format(now);
			
			for (String st : co_res2) {
				String[] re = st.split(",");
				pst.setString(1, re[0]);
				pst.setString(2, re[1]);
				pst.setInt(3, Integer.parseInt(re[2]));
				pst.setInt(4, Integer.parseInt(re[3]));
				pst.setString(5, date.format(day));
				pst.setString(6, date.format(day));
				pst.setString(7, create_time);
				pst.addBatch();
				if (++count % batchSize == 0) {
					try {
						pst.executeBatch();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			pst.executeBatch(); // insert remaining records
			pst.close();
			connection.close();

		}
	
	
	
	//省、市、区、产品类型（直播服务、刷脸考勤、RFID）、销售量、报表日期、创建日期
	public void SelledProduct(SparkSession session) throws SQLException {
		// 读取数据表
		session.read()
				.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_school", PropertiesUtil.getProperties())
				.toDF().createOrReplaceTempView("t_school");
		session.read()
				.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_card", PropertiesUtil.getProperties())
				.toDF().createOrReplaceTempView("t_card");
		
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_user_pay_detail",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_user_pay_detail");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_area",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_plat_area");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_near",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_plat_near");

		// 目的： 省、市、区、产品类型（直播服务=1 刷脸考勤=2 RFID=3）、销售量、实付款金额s_money、卡券金额s_card_money/报表日期、创建日期

		// 1.开通直播服务(+卡券金额s_card_money    实付款金额s_money) 转入正式库时服务代码需要修改f_increment_id='123523'123547
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_province",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_province");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_city",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_city");
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_area",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_area");

		session.sql("SELECT p,c,a,1 type,COUNT(*) co ,sum(money) money,sum(c_money) c_money FROM ("
				+ "SELECT t_zbo.s_money money,"
				+ "t_zbo.s_card_money c_money,"
				+ "t_zbo.p_id zbo,"
				+ "t_sc.f_province_id p,"
				+ "t_sc.f_city_id c,"
				+ "t_sc.f_area_id a "
				+ "FROM t_user_pay_detail t_zbo,t_school t_sc "
				+ "WHERE t_zbo.s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') and t_zbo.f_increment_id='123547' AND t_zbo.f_school_id=t_sc.p_id)tem GROUP BY p,c,a")
				.toDF().createOrReplaceTempView("t_zhibo_id");
		session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,1 type,co,t_1.money,t_1.c_money FROM t_zhibo_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
						+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id")
				.toDF().createOrReplaceTempView("t_zhibo_co");



		// 2.开通刷脸考勤
		session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_pingan_card",
				PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_pingan_card");

		session.sql("SELECT p,c,a,2 type,COUNT(*) co "
				+ "FROM (SELECT t_face.s_id p_id,t_sc.f_province_id p,t_sc.f_city_id c,t_sc.f_area_id a "
				+ "FROM t_pingan_card t_face,t_school t_sc " + "WHERE t_face.s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') and t_face.s_card_id <> '-1' "
				+ "AND t_face.f_school_id=t_sc.p_id)tmp " + "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_face_id");
		session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,2 type,co ,null money,null c_money "
				+ "FROM t_face_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
				+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id").toDF()
				.createOrReplaceTempView("t_face_co");

		// 3.RFID考勤卡
		session.sql("SELECT p,c,a,3 type,COUNT(*) co "
				+ "FROM (SELECT t_ca.p_id,t_sc.f_province_id p,t_sc.f_city_id c,t_sc.f_area_id a "
				+ "FROM t_card t_ca,t_school t_sc " + "WHERE t_ca.s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') and t_ca.s_card_id <> '-1' AND t_ca.f_school_id=t_sc.p_id)tmp "
				+ "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_card_id");
		session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,3 type,co ,null money,null c_money "
				+ "FROM t_card_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
				+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id").toDF()
				.createOrReplaceTempView("t_card_co");

		// 数据的整合
		List<Row> li_res = session.sql(
				"select * from t_zhibo_co UNION ALL "
				+ "select * from t_face_co UNION ALL "
				+ "select * from t_card_co").collectAsList();

		// 插入数据库
//		String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
//		String user = "root";
//		String password = "test3pass";

		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
		String user = "xxv2";
		String password = "xv2PassWD-321";
		
		
		
		String sql = "INSERT INTO t_report_selled_count (s_province_name,s_city_name,s_area_name,s_service_type,s_selled_count,s_money,s_card_money,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Connection connection = DriverManager.getConnection(url, user, password);
		PreparedStatement pst = connection.prepareStatement(sql);
		final int batchSize = 1000;
		int count = 0;

		Date now = new Date();

		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
		String create_time = time.format(now);
		now.setDate(now.getDate()-1);
		String report_date = date.format(now);
		
		

		for (Row st : li_res) {
			String[] re = ((String) st.toString().subSequence(1, st.toString().length() - 1)).split(",");
			pst.setString(1, re[0]);
			pst.setString(2, re[1]);
			pst.setString(3, re[2]);
			pst.setInt(4, Integer.parseInt(re[3]));//type
			pst.setInt(5, Integer.parseInt(re[4]));//liang
			if(re[5].equals(null) ||re[5].equals("null")){pst.setDouble(6, 0.00);}else{pst.setDouble(6, Double.parseDouble(re[5]));}//money
			if(re[6].equals(null) ||re[6].equals("null")){pst.setDouble(7, 0.00);}else{pst.setDouble(7, Double.parseDouble(re[6]));}//card_money
			pst.setString(8, report_date);
			pst.setString(9, create_time);
			pst.addBatch();
			if (++count % batchSize == 0) {
				try {
					pst.executeBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		pst.executeBatch(); // insert remaining records
		pst.close();
		connection.close();
	}
	
	
	
	//周边机构的量:省、市、区、销售量、报表日期、创建日期
	public void SelledNear(SparkSession session){
		
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_area", PropertiesUtil.getProperties())
		.toDF().createOrReplaceTempView("t_plat_area");
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_near", PropertiesUtil.getProperties())
		.toDF().createOrReplaceTempView("t_plat_near");
		
		// 2.开通周边机构
		session.sql("SELECT p_id,area_name province FROM t_plat_area").toDF().createOrReplaceTempView("t_p");
		session.sql("SELECT p_id,area_name city FROM t_plat_area").toDF().createOrReplaceTempView("t_c");
		session.sql("SELECT p_id,area_name area FROM t_plat_area").toDF().createOrReplaceTempView("t_a");

		session.sql(
				"SELECT s_province_id p,s_city_id c,s_area_id a,COUNT(*) co FROM t_plat_near WHERE s_state='1' and s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') GROUP BY s_province_id,s_city_id,s_area_id")
				.toDF().createOrReplaceTempView("t_near_co_1");
//		session.sql("SELECT tp.province p,tc.city c,ta.area a,2 type,co ,null money,null c_money " + "FROM t_near_co_1 t1,t_p tp,t_c tc,t_a ta "
//				+ "WHERE t1.p=tp.p_id and t1.c=tc.p_id and t1.a=ta.p_id").toDF().registerTempTable("t_near_co");
		
		
		
		Dataset<Row> li_res = session.sql(
				"SELECT tp.province s_province_name,"
				+ "tc.city s_city_name,"
				+ "ta.area s_area_name,"
				+ "co s_selled_count,"
				+ "0 s_money,"
				+ "0 s_card_money,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "FROM t_near_co_1 t1,t_p tp,t_c tc,t_a ta "
				+ "WHERE t1.p=tp.p_id and t1.c=tc.p_id and t1.a=ta.p_id");
		
		li_res.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_selled_near",
				PropertiesUtil.getProperties());
		
		
		
		
	}
	
	
	
	//省、市、区、学校类型（小学、初中、高中、幼儿园、培训机构）、学校量、报表日期、创建时间
	public void schoolType(SparkSession session) throws SQLException{
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_school", PropertiesUtil.getProperties())
		.toDF().createOrReplaceTempView("t_school");
		List<Row> df_res = session.sql("SELECT "
				+ "f_province_id,"
				+ "f_city_id,"
				+ "f_area_id,"
				+ "f_school_type,"
				+ "COUNT(*) f_school_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "FROM t_school where s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') GROUP BY f_province_id,f_city_id,f_area_id,f_school_type").collectAsList();
		
		
		// 插入数据库
//		String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
//		String user = "root";
//		String password = "test3pass";
		
		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
		String user = "xxv2";
		String password = "xv2PassWD-321";
		
		
		
		Connection connection2 = DriverManager.getConnection(url, user, password);
		String sql2 = "DELETE FROM t_report_school_type";		
		PreparedStatement pst0 = connection2.prepareStatement(sql2);
		pst0.addBatch();
		pst0.executeBatch(); // insert remaining records
		pst0.close();
		connection2.close();
		
		
		

		String sql = "INSERT INTO t_report_school_type (p_id,f_province_id,f_city_id,f_area_id,s_school_type,s_school_count,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		Connection connection = DriverManager.getConnection(url, user, password);	
		PreparedStatement pst = connection.prepareStatement(sql);
		final int batchSize = 1000;
		int count = 0;
		
		int p_id = 1;
		for (Row st : df_res) {
			String[] re = ((String) st.toString().subSequence(1, st.toString().length() - 1)).split(",");
			pst.setInt(1, p_id);
			pst.setString(2, re[0]);
			pst.setString(3, re[1]);
			pst.setString(4, re[2]);
			pst.setString(5, re[3]);
			pst.setInt(6, Integer.parseInt(re[4]));
			pst.setString(7, re[5]);
			pst.setString(8, re[6]);
			p_id++;
			pst.addBatch();
			if (++count % batchSize == 0) {
				try {
					pst.executeBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		pst.executeBatch(); // insert remaining records
		pst.close();
		connection.close();
		
	}
	
	
	
	//学校ID、班级数量、报表日期、创建时间
	public void classcount(SparkSession session) throws Exception{
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_class", PropertiesUtil.getProperties())
		.toDF().createOrReplaceTempView("t_class");
		List<Row> df_res = session.sql("SELECT "
				+ "f_school_id,"
				+ "COUNT(*) f_class_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time "
				+ "FROM t_class WHERE s_status=1 and s_create_time<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') GROUP BY f_school_id").collectAsList();
		
		
		// 插入数据库
//		String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
//		String user = "root";
//		String password = "test3pass";

		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
		String user = "xxv2";
		String password = "xv2PassWD-321";
		
		
		Connection connection2 = DriverManager.getConnection(url, user, password);
		String sql2 = "DELETE FROM t_report_class_count";		
		PreparedStatement pst0 = connection2.prepareStatement(sql2);
		pst0.addBatch();
		pst0.executeBatch(); // insert remaining records
		pst0.close();
		connection2.close();
		
		
		String sql = "INSERT INTO t_report_class_count (p_id,f_school_id,s_class_count,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?)";
		Connection connection = DriverManager.getConnection(url, user, password);
		PreparedStatement pst = connection.prepareStatement(sql);
		final int batchSize = 1000;
		int count = 0;

//		Date now = new Date();
//		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
//		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
//		String report_date = date.format(now);
//		String create_time = time.format(now);
		
		int p_id = 1;
		for (Row st : df_res) {
			String[] re = ((String) st.toString().subSequence(1, st.toString().length() - 1)).split(",");
			pst.setInt(1, p_id);
			pst.setString(2, re[0]);
			pst.setInt(3, Integer.parseInt(re[1]));
			pst.setString(4, re[2]);
			pst.setString(5, re[3]);
			p_id++;
			pst.addBatch();
			if (++count % batchSize == 0) {
				try {
					pst.executeBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		pst.executeBatch(); // insert remaining records
		pst.close();
		connection.close();
		
	}
	
	
	
	//分角色统计用户日活跃情况
	public void userTypeDau(SparkSession session){
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_user_report", PropertiesUtil.getProperties())
		.createOrReplaceTempView("t_user_report");
		
		Date now = new Date();
		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		String today = time.format(now);
		now.setDate(now.getDate()-1);
		String yesterday = time.format(now);
		Dataset<Row> sqlDF = session.sql("SELECT "
				+ "f_province_id,"
				+ "f_city_id,"
				+ "f_area_id,"
				+ "s_user_type,"
				+ "COUNT(*) s_active_user_count,"
				+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') s_report_date,"
				+ "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "FROM t_user_report "
				+ "WHERE s_report_date = '" + yesterday + "' "
				+ "GROUP BY f_city_id,f_area_id,s_user_type,f_province_id ");

		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_usertype_active",
				PropertiesUtil.getProperties());
		
		
	}
	
	
	
	
	
	
	//统计学校服务的开通状况
	public void schoolService(SparkSession session)throws Exception{
		// 读取数据表
				session.read()
						.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_school", PropertiesUtil.getProperties())
						.toDF().createOrReplaceTempView("t_school");
				session.read()
						.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_card", PropertiesUtil.getProperties())
						.toDF().createOrReplaceTempView("t_card");
				
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_user_pay_detail",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_user_pay_detail");
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_area",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_plat_area");
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_plat_near",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_plat_near");

				// 目的： 省、市、区、产品类型（直播服务=1 刷脸考勤=2 RFID=3）、销售量、使用比例（开通服务人数/区域总人数（已注册））、报表日期、创建日期

				// 1.开通直播服务(+卡券金额s_card_money    实付款金额s_money) 转入正式库时服务代码需要修改f_increment_id='123547'
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_province",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_province");
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_city",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_city");
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_sys_area",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_sys_area");

				
				
				
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co FROM ("
						+ "SELECT t_zbo.s_money money,"
						+ "t_zbo.s_card_money c_money,"
						+ "t_zbo.p_id zbo,"
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM t_user_pay_detail t_zbo,t_school t_sc "
						+ "WHERE t_zbo.f_increment_id='123547' and " 
//						+ "t_zbo.s_create_time>'2017-01-01 00:00:00' and "
						+ "t_zbo.s_create_time<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') "
						+ "AND t_zbo.f_school_id=t_sc.p_id)tem GROUP BY p,c,a")
						.toDF().createOrReplaceTempView("t_zhibo_id1");
				
				
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co FROM ("
						+ "SELECT t_zbo.s_money money,"
						+ "t_zbo.s_card_money c_money,"
						+ "t_zbo.p_id zbo,"
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM t_user_pay_detail t_zbo,t_school t_sc "
						+ "WHERE t_zbo.f_increment_id='123547' and " 
//						+ "t_zbo.s_create_time>'2017-01-01 00:00:00' and "
						+ "t_zbo.s_create_time<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') "
						+ "AND t_zbo.f_school_id=t_sc.p_id)tem GROUP BY p,c,a")
						.toDF().createOrReplaceTempView("t_zhibo_id2");
				
				session.sql("select t_1.p p,t_1.c c,t_1.a a,t_1.co co1,t_2.co co2 from t_zhibo_id1 t_1,t_zhibo_id2 t_2 where t_1.a=t_2.a")
						.toDF().createOrReplaceTempView("t_zhibo_id");
				
				session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,1 type,t_1.co1,t_1.co2 FROM t_zhibo_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
								+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id")
						.toDF().createOrReplaceTempView("t_zhibo_co");
				
				
				
				
				
				
				
				//省，市，区，1，绑定学校量co1
				//省，市，区，1，使用学校量co2



				// 2.开通刷脸考勤
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_pingan_card",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_pingan_card");
				session.read().jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_student_class_map",
						PropertiesUtil.getProperties()).toDF().createOrReplaceTempView("t_student_class_map");
				
				Properties propt = new Properties();
				propt.put("user","root");
				propt.put("password","test1pass");
				
				session.read().jdbc("jdbc:mysql://192.168.0.16:3306/exiaoxincard?characterEncoding=utf-8", "t_pingan_card_checkInfo",
						propt).toDF().createOrReplaceTempView("t_pingan_card_checkInfo");
				
				
				
				//绑定学校量
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co "
						+ "FROM ("
						+ "SELECT t_face.s_id p_id,"
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM t_pingan_card t_face,t_school t_sc "
						+ "WHERE "
						+ "t_face.s_card_id <> '-1' "	
//						+ "and t_face.s_create_time>'2017-01-01 00:00:00' "
						+ "and t_face.s_create_time<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') "
						+ "AND "
						+ "t_face.f_school_id=t_sc.p_id)tmp "
						+ "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_face_id1");

				//使用学校量
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co "
						+ "FROM ("
						+ "SELECT "
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM t_pingan_card_checkInfo t_face,t_student_class_map t_st,t_school t_sc "
						+ "WHERE "
						+ "t_face.f_card_id <> '-1' "
						+ "and t_face.f_card_id <> '0' "
						+ "and t_face.f_student_id=t_st.f_student_id "	
						+ "and t_st.f_school_id=t_sc.p_id "
//						+ "and t_face.s_create_time>'2017-01-01 00:00:00' "
						+ "and t_face.s_time<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') )tmp "
						+ "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_face_id2");
				
				session.sql("select t_1.p p,t_1.c c,t_1.a a,t_1.co co1,t_2.co co2 from t_face_id1 t_1 LEFT JOIN t_face_id2 t_2 ON t_1.a=t_2.a")
						.toDF().createOrReplaceTempView("t_face_id");
				
				session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,2 type,t_1.co1,t_1.co2 "
						+ "FROM t_face_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
						+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id").toDF()
						.createOrReplaceTempView("t_face_co");
				
				//省，市，区，2，绑定学校量
				//省，市，区，2，使用学校量
				
				
				

				
				
				// 3.RFID考勤卡
				
				session.read()
					.jdbc("jdbc:mysql://192.168.0.16:3306/information_schema?characterEncoding=utf-8", "tables", propt)
					.toDF().createOrReplaceTempView("tables");
				//绑卡学校量
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co "
						+ "FROM ("
						+ "SELECT t_ca.p_id,"
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM "
						+ "t_card t_ca,"
						+ "t_school t_sc "
						+ "WHERE "
						+ "t_ca.s_card_id <> '-1' "
//						+ "AND t_ca.s_create_time>'2017-01-01 00:00:00' "
						+ "AND t_ca.s_create_time<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') "
						+ "AND "
						+ "t_ca.f_school_id=t_sc.p_id)tmp "
						+ "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_card_id1");
				
				
				
				
				
				//使用学校量
				
				//找到了学校
				session.sql("SELECT SUBSTRING_INDEX(table_name,'o_',-1) f_school_id,CREATE_TIME FROM TABLES WHERE table_name LIKE 't_card_checkInfo_%-%' AND table_type='base table' ")
					.toDF().createOrReplaceTempView("t_sc_list");
				
				session.sql("SELECT p,c,a,COUNT(distinct school_id) co "
						+ "FROM ("
						+ "SELECT "
						+ "t_sc.f_province_id p,"
						+ "t_sc.f_city_id c,"
						+ "t_sc.f_area_id a,"
						+ "t_sc.p_id school_id "
						+ "FROM "
						+ "t_sc_list t_ca,"
						+ "t_school t_sc "
						+ "WHERE "
						+ "t_ca.CREATE_TIME<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00') "
						+ "AND "
						+ "t_ca.f_school_id=t_sc.p_id)tmp "
						+ "GROUP BY p,c,a").toDF().createOrReplaceTempView("t_card_id2");
				
				
				
				
			
				
				
				session.sql("select t_1.p p,t_1.c c,t_1.a a,t_1.co co1,t_2.co co2 from t_card_id1 t_1 LEFT JOIN t_card_id2 t_2 ON t_1.a=t_2.a")
					.toDF().createOrReplaceTempView("t_card_id");
				
				
				
				
				
				session.sql("SELECT t_p.s_name p,t_c.s_name c,t_a.s_name a,3 type,t_1.co1,t_1.co2 "
						+ "FROM t_card_id t_1,t_sys_province t_p,t_sys_city t_c,t_sys_area t_a "
						+ "WHERE t_1.p=t_p.p_id AND t_1.c=t_c.p_id " + "AND t_1.a=t_a.p_id").toDF()
						.createOrReplaceTempView("t_card_co");
				
				//省，市，区，3，绑定学校量
				//省，市，区，4，使用学校量
				
				

				// 数据的整合
				List<Row> li_res = session.sql(
						"select * from t_zhibo_co UNION ALL "
						+ "select * from t_face_co UNION ALL "
						+ "select * from t_card_co").collectAsList();

				// 插入数据库
//				String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
//				String user = "root";
//				String password = "test3pass";

				String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
				String user = "xxv2";
				String password = "xv2PassWD-321";
				
				
				

				String sql = "INSERT INTO t_report_selled_school_count (s_province_name,s_city_name,s_area_name,s_service_type,s_school_count,s_use_school_count,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
				Connection connection = DriverManager.getConnection(url, user, password);
				PreparedStatement pst = connection.prepareStatement(sql);
				final int batchSize = 1000;
				int count = 0;

				Date now = new Date();

				SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
				SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式

				String create_time = time.format(now);
				now.setDate(now.getDate()-1);
				String report_date = date.format(now);
				

				for (Row st : li_res) {
					String[] re = ((String) st.toString().substring(1, st.toString().length() - 1)).split(",");
					pst.setString(1, re[0]);
					pst.setString(2, re[1]);
					pst.setString(3, re[2]);
					pst.setInt(4, Integer.parseInt(re[3]));//type
					pst.setInt(5, Integer.parseInt(re[4]));//liang bangding
					if(re[5].equals("null")){pst.setInt(6, 0);}else{pst.setInt(6,Integer.parseInt(re[5]));}//liang shiyong
					pst.setString(7, report_date);
					pst.setString(8, create_time);
					pst.addBatch();
					if (++count % batchSize == 0) {
						try {
							pst.executeBatch();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				pst.executeBatch(); // insert remaining records
				pst.close();
				connection.close();
		
	}

	
	
	//---------------------------------------------------------------------------------
	
//	
//	public void schoolCardtmp(SparkSession session) throws SQLException {
//		// 读取数据表
//		Properties propt = new Properties();
//		propt.put("user","root");
//		propt.put("password","test1pass");
//		
//		
//		session.read()
//				.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_school", PropertiesUtil.getProperties())
//				.toDF().createOrReplaceTempView("t_school");
//		session.read()
//				.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_card", PropertiesUtil.getProperties())
//				.toDF().createOrReplaceTempView("t_card");
//		session.read()
//				.jdbc("jdbc:mysql://192.168.0.16:3306/information_schema?characterEncoding=utf-8", "tables", propt)
//				.toDF().createOrReplaceTempView("tables");
//
//		// 目的：学校ID、学校名称、总绑卡量、有效打卡量、打卡日期、报表日期、创建时间
//
//		// 表名列表
//		JavaRDD<Row> javaRDD1 = session
//				.sql("SELECT table_name FROM tables WHERE table_name LIKE 't_card_checkInfo_%-%' AND table_type='base table'")
//				.toJavaRDD();
//		JavaRDD<String> table_names = javaRDD1.map(new Function<Row, String>() {
//			@Override
//			public String call(Row row) throws Exception {
//				return row.getString(0);
//			}
//		});
//
//		// 总绑卡数
//		List<Row> co_card = session.sql("SELECT f_school_id,COUNT(*) FROM t_card GROUP BY f_school_id").toDF()
//				.collectAsList();
//
//		
//		
//		Date day = new Date();
//		SimpleDateFormat zero_time = new SimpleDateFormat("yyyy-MM-dd 00:00:00");// 设置日期格式
//		String today = zero_time.format(day);
//		day.setDate(day.getDate()-1);
//		String yesterday = zero_time.format(day);
//
//		
//		//17日一天的 当前yesterday today
//		//17日之前7天的 "2017-05-10 00:00:00"  "2017-05-17 00:00:00"
//		//17日之前一个月的"2017-04-17 00:00:00" "2017-05-17 00:00:00"
//		
//		
//		
//		
//		
//		// 有效考勤数
//		List<String> co_kaoqin = new ArrayList<String>();
//		for (String table_name : table_names.collect()) {
//			session.read().jdbc("jdbc:mysql://192.168.0.16:3306/exiaoxincard?characterEncoding=utf-8", "`" + table_name + "`",
//					propt).toDF().createOrReplaceTempView("t_school_card");
//
//			session.read().jdbc("jdbc:mysql://192.168.0.16:3306/exiaoxincard?characterEncoding=utf-8", "`" + table_name + "`",
//					new String[]{"s_time >= '"+ "2017-04-17 00:00:00" + "' and s_time < '" + "2017-05-17 00:00:00" + "'"}, propt).toDF().registerTempTable("t_school_card");
//				String count_card = session.sql("SELECT COUNT(*) FROM t_school_card WHERE s_type=0").first().toString();//入校量1，出校量0
//			co_kaoqin.add(table_name.substring(17, 53) + "," + count_card.subSequence(1, count_card.length() - 1));
//
//		}
//
//		// 学校信息表
//		List<Row> co_school = session.sql("SELECT p_id,s_name FROM t_school where f_area_id='71acacd0-3cb0-4004-86be-516f1e09f8bf'").toDF().collectAsList();
//
//		// 学校id，绑卡数，打卡数
//		List<String> co_res1 = new ArrayList<String>();
//		for (String str : co_kaoqin) {
//			String[] kao = str.split(",");
//			for (Row row : co_card) {
//				String[] card = row.toString().substring(1, row.toString().length() - 1).split(",");
//				if (kao[0].equals(card[0])) {
//					co_res1.add(kao[0] + "," + card[1] + "," + kao[1]);
//				}
//			}
//		}
//
//		// 将结果进行拼
//		List<String> co_res2 = new ArrayList<String>();
//		for (String str : co_res1) {
//			String[] res1 = str.split(",");
//			for (Row row : co_school) {
//				String[] sc = row.toString().substring(1, row.toString().length() - 1).split(",");
//				if (res1[0].equals(sc[0])) {
//
//					co_res2.add(sc[0] + "," + sc[1] + "," + res1[1] + "," + res1[2]);
//
//				}
//			}
//		}
//
//		// 插入数据库
////		String url = "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8";
////		String user = "root";
////		String password = "test3pass";
//
//		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
//		String user = "xxv2";
//		String password = "xv2PassWD-321";
//		
//
//		String sql = "INSERT INTO t_report_school_card_tmp(f_school_id,s_school_name,s_card_count,s_clock_count,s_clock_date,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
//		Connection connection = DriverManager.getConnection(url, user, password);
//		PreparedStatement pst = connection.prepareStatement(sql);
//		final int batchSize = 1000;
//		int count = 0;
//		Date now = new Date();
//		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
//		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
//
//		String create_time = time.format(now);
//		
//		for (String st : co_res2) {
//			String[] re = st.split(",");
//			pst.setString(1, re[0]);
//			pst.setString(2, re[1]);
//			pst.setInt(3, Integer.parseInt(re[2]));
//			pst.setInt(4, Integer.parseInt(re[3]));
//			pst.setString(5, date.format(day));
//			pst.setString(6, date.format(day));
//			pst.setString(7, create_time);
//			pst.addBatch();
//			if (++count % batchSize == 0) {
//				try {
//					pst.executeBatch();
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
//		pst.executeBatch(); // insert remaining records
//		pst.close();
//		connection.close();
//		
//		
//		
//		
//	}
//	
	
	//2017-06-02----下
	
	
	public void pageCount(SparkSession session)throws Exception{
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		
		//构思结果表t_report_page_count：p_id,s_pagename,s_view_count,s_report_date,s_create_time
		
		
			Dataset<Row> sqlDF = session.sql("select "
					+ "pagename s_pagename,"
					+ "count(*) s_view_count,"
					+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
					+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
					+ "from pagedata "
					+ "where pagename <> '' "
					+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
					+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
					+ "and pagename is not null "
					+ "group by pagename");
			sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_page_count",PropertiesUtil.getProperties());
		
			
			
	}
	
	public void openCount(SparkSession session){
		//将sql语句的查询结果存储在一个Dataset中，进而通过jdbc导出到mysql数据库中
		
		//构思结果表   `s_open_count`,`s_count_type`,`s_report_date`,`s_create_time
		
		Dataset<Row> sqlDF = session.sql("select "
				+ "count(*) s_open_count,"
				+ "'d' s_count_type,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "from logindata "
				+ "where "
				+ "logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') ");
		
		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_open_count",PropertiesUtil.getProperties());

		
		//获取周启动次数
				Calendar ca = Calendar.getInstance();
				ca.setTime(new Date());
				int week = ca.get(Calendar.DAY_OF_WEEK);
				if(week == 2){
					Dataset<Row> sqlDF1 = session.sql("select "
							+ "count(*) s_open_count,"
							+ "'w' s_count_type,"
							+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
							+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
							+ "from logindata "
							+ "where "
							+ "logintime>from_unixtime(unix_timestamp()-86400*7,'yyyy-MM-dd 00:00:00') "
							+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') ");
					sqlDF1.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_open_count",PropertiesUtil.getProperties());
				}
				
		
				//统计月启动次数
				SimpleDateFormat getdate = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();//得到一个Calendar的实例 
				cal.setTime(new Date()); //设置时间为当前时间 
				int day = cal.get(Calendar.DAY_OF_MONTH);
				if(day==1){
					cal.add(Calendar.MONTH, -1); //月份减1
					Date beforemonth = cal.getTime();
					String beforedate = getdate.format(beforemonth);					
					Dataset<Row> sqlDF2 = session.sql("select "
							+ "count(*) s_open_count,"
							+ "'m' s_count_type,"
							+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
							+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
							+ "from logindata "
							+ "where "
							+ "logintime > '"
							+ beforedate + " 00:00:00' "
							+ "and logintime < from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') ");
					sqlDF2.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_open_count",PropertiesUtil.getProperties());
				}
				
	}
	
	public String[] toarr(Row aa){
		
		return aa.toString().substring(1, aa.toString().length()-1).split(",");
	}
	
	public void trackCount(SparkSession session)throws Exception{
		List<Row> uplt = session.sql("select t1.userid,"
				+ "t1.pagename,t2.layer,t1.logintime "
				+ "from pagedata t1 INNER JOIN page_layer_map t2 on t1.pagename=t2.pagename where logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') order by t1.userid asc,t1.logintime asc ")
		.collectAsList();
		
		//创建结果集合
		List<String> user_track = new ArrayList<String>();
		
		
		
		//这种逻辑适合处理分好组的用户 ok 已经排序
		for(int a=0;a<(uplt.size()-9);a++){
			
			
			String[] aa1 = toarr(uplt.get(a));
			String[] aa2 = toarr(uplt.get(a+1));
			String[] aa3 = toarr(uplt.get(a+2));
			String[] aa4 = toarr(uplt.get(a+3));
			String[] aa5 = toarr(uplt.get(a+4));
			String[] aa6 = toarr(uplt.get(a+5));
			String[] aa7 = toarr(uplt.get(a+6));
			String[] aa8 = toarr(uplt.get(a+7));
			String[] aa9 = toarr(uplt.get(a+8));
			

			if(aa1[0].equals(aa2[0])){//第一个与第二个的用户名相等    uplt
				if(aa2[0].equals(aa3[0])){
					if(aa3[0].equals(aa4[0])){
						if(aa4[0].equals(aa5[0])){	
							if(aa5[0].equals(aa6[0])){
								if(aa6[0].equals(aa7[0])){
									if(aa7[0].equals(aa8[0])){
										if(aa8[0].equals(aa9[0])){//9
											if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123432345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123434345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123234345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123232345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12343434")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323434")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323234")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
											else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
											
											
											
											
											
										}else{//8
											if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12343434")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323434")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323234")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
											else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
											
											
										}
	
									}else{//7
										if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
											user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
										else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
										
										
										
									}
									
								}else{//6
									if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
									else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
									else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
									else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
										user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
									else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
									else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
									else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
									
									
								}
							
							}else{//5
								if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
								else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
									user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
									user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
								else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
								else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
								else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
								
								
							}
							
							
							
						}else{//4
							if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
							else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
							else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
							
							
						}
					}else{//3
						if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
						else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
						
					}
				}else{//2
					if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
					
				}
			}else{//1
				
			}
					
				
		}
		   
		List<String> user_track_count = new ArrayList<String>();  
		Set  set_user_track = new HashSet();
		
		
		for(String u_t : user_track){
			
			set_user_track.add(u_t);
		}
		
		for(Object u_t : set_user_track){
			user_track_count.add(u_t+ "," +Collections.frequency(user_track, u_t));
		}
		
		//读取用户浏览轨迹中间结果数据
		String schemaString2 = "f_user_id s_track_line s_track_count";
		List<StructField> fields2 = new ArrayList<StructField>();
		for (String fieldName : schemaString2.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields2.add(field);
		}
		
		StructType schema2 = DataTypes.createStructType(fields2);
		List<Row> user_track_count2 = new ArrayList<Row>();  
		for(String a:user_track_count){
			String[] attributes = a.split(",");
			if (attributes.length == 3 && !attributes[2].equals("")) {
				user_track_count2.add(RowFactory.create(attributes[0], attributes[1], attributes[2]));
			}
		}
		
		Dataset<Row> peopleDataFrame2 = session.createDataFrame(user_track_count2, schema2);
		// Creates a temporary view using the DataFram
		peopleDataFrame2.createOrReplaceTempView("t_report_track_count");
		session.read()
		.jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_user_base", PropertiesUtil.getProperties())
		.toDF().createOrReplaceTempView("t_user_base");
		
		//ios
		Dataset<Row> sqlDF2 = session.sql("SELECT t2.s_xiaoxincode,"
				+ "t2.s_user_name,"
				+ "t2.s_phone1,"
				+ "t2.s_type,"
				+ "'ios' s_os_type,"
				+ "t1.s_track_line,"
				+ "t1.s_track_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "FROM t_report_track_count t1,t_user_base t2 "
				+ "where t1.f_user_id=t2.p_id and (SUBSTRING_INDEX(t1.s_track_line,'C',-1)='ontroller' or SUBSTRING_INDEX(t1.s_track_line,'V',-1)='C') ");
		
		sqlDF2.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_track_count",PropertiesUtil.getProperties());

		//android
		Dataset<Row> sqlDF3 = session.sql("SELECT t2.s_xiaoxincode,"
				+ "t2.s_user_name,"
				+ "t2.s_phone1,"
				+ "t2.s_type,"
				+ "'android' s_os_type,"
				+ "t1.s_track_line,"
				+ "t1.s_track_count,"
				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
				+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
				+ "FROM t_report_track_count t1,t_user_base t2 "
				+ "where t1.f_user_id=t2.p_id and (SUBSTRING_INDEX(t1.s_track_line,'r',-1)='agment' or SUBSTRING_INDEX(t1.s_track_line,'A',-1)='ctivity' ) ");
		
		sqlDF3.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_track_count",PropertiesUtil.getProperties());

		


	}
	

	public void daysTrackCount(SparkSession session)throws Exception{
			
		//用hive数据源
//		session.sql("select t1.userid,t1.pagename,t2.layer,t1.logintime "
//				+ "from pagedata t1 INNER JOIN page_layer_map t2 "
//				+ "on t1.pagename=t2.pagename "
//				+ "where t1.logintime>FROM_UNIXTIME(UNIX_TIMESTAMP()-86400*20,'yyyy-MM-dd 00:00:00') "
//				+ "and t1.logintime<FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd 00:00:00')")
//		.createOrReplaceTempView("tmp1");
//		
//		
//		List<Row> uplt = session.sql("select userid,pagename,layer,logintime "
//				+ "from tmp1 "
//				+ "order by userid asc,logintime asc ")
//				.collectAsList();
//		
//		
		Date now = new Date();
		SimpleDateFormat date = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
		now.setDate(now.getDate()-1);
		String data_date = date.format(now);

		List<String> list_run = new ArrayList<String>();
		list_run.add("hdfs://master:9000/hive_SDKdata/pagedata/pagedata"+ data_date +",1day");
		list_run.add("hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata7day.txt,7day");
		list_run.add("hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata7day.txt,7day");
		list_run.add("hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata30day.txt,30day");
		list_run.add("hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata30day.txt,30day");
		
		
		for(String oneRun : list_run){
			String[] url_day = oneRun.split(",");
			String url = url_day[0];
			String day = url_day[1];
			
			
			
			
			
			//用dfs数据源
			//读取页面浏览数据																/user/hadoop/days_pagedata/android/pagedata30day.txt
			JavaRDD<String> pageRDD = session.sparkContext().textFile(url, 5)
					.toJavaRDD();
	
			String schemaString = "userid pagename functionname logintime logouttime";
			List<StructField> fields = new ArrayList<StructField>();
			for (String fieldName : schemaString.split(" ")) {
				StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
				fields.add(field);
			}
	
			StructType schema = DataTypes.createStructType(fields);
			System.gc();
			JavaRDD<Row> rowRDD = pageRDD.map(new Function<String, Row>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Row call(String row) throws Exception {
					String[] attributes = row.split(",");
					if (attributes.length == 5 ) {
						return RowFactory.create(attributes[0], attributes[1],attributes[2], attributes[3], attributes[4]);
					} else {
						return RowFactory.create(null, null, null, null, null);
					}
				}
			});
			System.gc();
			Dataset<Row> peopleDataFrame = session.createDataFrame(rowRDD, schema);
			// Creates a temporary view using the DataFram
			peopleDataFrame.createOrReplaceTempView("t_pagedata");
			List<Row> uplt = session.sql("select t1.userid,t1.pagename,t2.layer,t1.logintime from t_pagedata t1 INNER JOIN page_layer_map t2 on t1.pagename=t2.pagename order by t1.userid asc,t1.logintime asc ")
					.collectAsList();
			
	
			
	//		for(Row r:uplt ){System.out.println(r);}
			
			
			//创建结果集合
			List<String> user_track = new ArrayList<String>();
			
			
			//这种逻辑适合处理分好组的用户 ok 已经排序
					for(int a=0;a<(uplt.size()-9);a++){
						
						
						String[] aa1 = toarr(uplt.get(a));
						String[] aa2 = toarr(uplt.get(a+1));
						String[] aa3 = toarr(uplt.get(a+2));
						String[] aa4 = toarr(uplt.get(a+3));
						String[] aa5 = toarr(uplt.get(a+4));
						String[] aa6 = toarr(uplt.get(a+5));
						String[] aa7 = toarr(uplt.get(a+6));
						String[] aa8 = toarr(uplt.get(a+7));
						String[] aa9 = toarr(uplt.get(a+8));
						
	
						if(aa1[0].equals(aa2[0])){//第一个与第二个的用户名相等    uplt	 
							if(aa2[0].equals(aa3[0])){
								if(aa3[0].equals(aa4[0])){
									if(aa4[0].equals(aa5[0])){	
										if(aa5[0].equals(aa6[0])){
											if(aa6[0].equals(aa7[0])){
												if(aa7[0].equals(aa8[0])){
													if(aa8[0].equals(aa9[0])){//9
														if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123432345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123434345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123234345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]+aa9[2]).equals("123232345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]+"-"+aa9[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12343434")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323434")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323234")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
														else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
															
													}else{//8
														if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12343434")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323434")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]+aa8[2]).equals("12323234")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa6[1]+"-"+aa7[1]+"-"+aa8[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
															user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
														else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
														else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
														
														
													}
	
												}else{//7
													if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1232345")){
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]+aa7[2]).equals("1234345")){
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]+"-"+aa7[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
														user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
													else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
													else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
													
													
													
												}
												
											}else{//6
												if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123434")) {
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa5[1]+"-"+aa6[1]);}
												else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]+aa6[2]).equals("123234")) {
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]+"-"+aa6[1]);}
												else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
												else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
													user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
												else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
												else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
												else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
												
												
											}
										
										}else{//5
											if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12345")){user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]+aa5[2]).equals("12323")){
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);
												user_track.add(aa1[0]+","+aa1[1]+"-"+aa4[1]+"-"+aa5[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
											else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
											else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
											
											
										}
										
										
										
									}else{//4
										if((aa1[2]+aa2[2]+aa3[2]+aa4[2]).equals("1234")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]+"-"+aa4[1]);}
										else if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
										else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
										
										
									}
								}else{//3
									if((aa1[2]+aa2[2]+aa3[2]).equals("123")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]+"-"+aa3[1]);}
									else if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
									
								}
							}else{//2
								if((aa1[2]+aa2[2]).equals("12")) {user_track.add(aa1[0]+","+aa1[1]+"-"+aa2[1]);}
								
							}
						}else{//1
							
						}
							
						
					}
	
			   
			List<String> user_track_count = new ArrayList<String>();  
			Set  set_user_track = new HashSet();
			
			
			for(String u_t : user_track){
				
				set_user_track.add(u_t);
			}
			
			for(Object u_t : set_user_track){
				user_track_count.add(u_t+ "," +Collections.frequency(user_track, u_t));
			}
			
			
	
			//读取用户浏览轨迹中间结果数据
			String schemaString2 = "f_user_id s_track_line s_track_count";
			List<StructField> fields2 = new ArrayList<StructField>();
			for (String fieldName : schemaString2.split(" ")) {
				StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
				fields2.add(field);
			}
			
			StructType schema2 = DataTypes.createStructType(fields2);
			List<Row> user_track_count2 = new ArrayList<Row>();  
			for(String a:user_track_count){
				String[] attributes = a.split(",");
				if (attributes.length == 3 && !attributes[2].equals("")) {
					user_track_count2.add(RowFactory.create(attributes[0], attributes[1], attributes[2]));
				}
			}
			
			Dataset<Row> peopleDataFrame2 = session.createDataFrame(user_track_count2, schema2);
			// Creates a temporary view using the DataFram
			peopleDataFrame2.createOrReplaceTempView("t_report_track_count");
		
			
			//ios
			Dataset<Row> sqlDF2 = session.sql("SELECT s_track_line,"
					+ "SUM(s_track_count) s_track_count,"
					+ "'" + day +"' s_count_type,"
					+ "'ios' s_os_type,"
					+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
					+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
					+ "FROM t_report_track_count where SUBSTRING_INDEX(s_track_line,'C',-1)='ontroller' or SUBSTRING_INDEX(s_track_line,'V',-1)='C' GROUP BY s_track_line ");
			sqlDF2.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_all_track",PropertiesUtil.getProperties());
			
			//android
			Dataset<Row> sqlDF3 = session.sql("SELECT s_track_line,"
					+ "SUM(s_track_count) s_track_count,"
					+ "'" + day +"' s_count_type,"
					+ "'android' s_os_type,"
					+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
					+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
					+ "FROM t_report_track_count where SUBSTRING_INDEX(s_track_line,'r',-1)='agment' or SUBSTRING_INDEX(s_track_line,'A',-1)='ctivity' GROUP BY s_track_line ");
			sqlDF3.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_all_track",PropertiesUtil.getProperties());
			
			
			
			
			
		}	
		//构造结果表：
		/* t_report_all_track
		 * 
		 * p_id,
		 * s_track_line,
		 * s_count_type,
		 * s_track_count,
		 * s_report_date,
		 * s_create_time
		 */
		
	
//		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
//		String user = "xxv2";
//		String password = "xv2PassWD-321";
//		
////		String url = "jdbc:mysql://122.193.22.133:3311/exiaoxin?characterEncoding=utf-8";
////		String user = "root";
////		String password = "123456";
//
//		
//		
//		/*t_report_track_count
//
//		p_id
//		f_user_id
//		s_track_line
//		s_track_count
//		s_report_date
//		s_create_time
//		 */
//
//		String sql = "INSERT INTO t_report_track_count (f_user_id,s_track_line,s_track_count,s_report_date,s_create_time) VALUES (?, ?, ?, ?, ?)";
//		Connection connection = DriverManager.getConnection(url, user, password);
//		PreparedStatement pst = connection.prepareStatement(sql);
//		final int batchSize = 1000;
//		int count = 0;
//
//		Date now = new Date();
//
//		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
//		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
//
//		String create_time = time.format(now);
//		now.setDate(now.getDate()-1);
//		String report_date = date.format(now);
//		
//
//		for (String st : user_track_count) {
//			
//			String[] re = st.split(",");
//			if(re.length==3){
//				pst.setString(1, re[0]);
//				pst.setString(2, re[1]);
//				pst.setInt(3, Integer.parseInt(re[2]));
//				pst.setString(4, report_date);
//				pst.setString(5, create_time);
//				pst.addBatch();
//				if (++count % batchSize == 0) {
//					try {
//						pst.executeBatch();
//					} catch (SQLException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				
//			}
//			
//		}
//		pst.executeBatch(); // insert remaining records
//		pst.close();
//		connection.close();

	}
	
	
	public String sub(String line){	return line.substring(1, line.length()-1);}
	
	public void apiMonitor(SparkSession session) throws SQLException{
		
		//结果表设计
		/*
		 * p_id，有效签到数，总签到数，有效发作业，总发作业，有效发通知，总发通知，有效发动态，总发动态，有效点赞，总点赞数,报表日期,创建时间
		 * 
			签到：api/v2/pointdetail，
			发作业：api/v2/classes/{subjectid}/createhomework，
			发通知：api/v2/classes/createnotice，
			发动态：api/v2/classes/{cid}/events，
			点赞：api/v2/classes/{classId}/events/{eventId}
		 */
		
		
		session.sql("select s_user_id,s_eventdesc,s_eventtime from eventlog "
				+ "where s_eventtime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
				+ "and s_eventtime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
				+ "and (substring_index(s_eventdesc,'/',-1) = 'pointdetail' "//签到
				+ "or substring_index(s_eventdesc,'/',-1) = 'createhomework' "//发作业
				+ "or s_eventdesc = '/api/v2/classes/createnotice' "//发通知
				+ "or substring_index(s_eventdesc,'/',-1) = 'events' "//发动态
				+ "or substring_index(substring_index(s_eventdesc,'/',-2),'/',1) = 'events' )")//点赞
		.createOrReplaceTempView("t_eventlog");
		
		String qiandao = sub(session.sql("select count(distinct s_user_id) youxiao,count(s_user_id) zong from t_eventlog where substring_index(s_eventdesc,'/',-1) = 'pointdetail' ").collectAsList().get(0).toString());
		String homework = sub(session.sql("select count(distinct s_user_id) youxiao,count(s_user_id) zong from t_eventlog where substring_index(s_eventdesc,'/',-1) = 'createhomework' ").collectAsList().get(0).toString());
		String notice = sub(session.sql("select count(distinct s_user_id) youxiao,count(s_user_id) zong from t_eventlog where s_eventdesc = '/api/v2/classes/createnotice' ").collectAsList().get(0).toString());
		String dongtai = sub(session.sql("select count(distinct s_user_id) youxiao,count(s_user_id) zong from t_eventlog where substring_index(s_eventdesc,'/',-1) = 'events' ").collectAsList().get(0).toString());
		String zan_youxiao = sub(session.sql("SELECT SUM(co) youxiao FROM (SELECT s_user_id,COUNT(*) co FROM t_eventlog WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(s_eventdesc,'/',-2),'/',1) = 'events' GROUP BY s_user_id)t WHERE co<51").collectAsList().get(0).toString());
		String zan_zong = sub(session.sql("SELECT SUM(co) zong FROM (SELECT s_user_id,COUNT(*) co FROM t_eventlog WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(s_eventdesc,'/',-2),'/',1) = 'events' GROUP BY s_user_id)t").collectAsList().get(0).toString());
		
		
		
		String result = qiandao+ ","+homework+ ","+notice+ ","+dongtai+ ","+zan_youxiao+ ","+zan_zong;
		//[10557,10565],[1394,1525],[1085,1162],[1086,1357],[3542,19295]
		//需要对点赞有效数进行再处理 ok
		String[] re = result.split(",");
		String url = "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8";
		String user = "xxv2";
		String password = "xv2PassWD-321";
		
		String sql = "INSERT INTO t_report_apimonitor ("
				+ "s_qiandao_eff,"
				+ "s_qiandao_sum,"
				+ "s_homework_eff,"
				+ "s_homework_sum,"
				+ "s_notice_eff,"
				+ "s_notice_sum,"
				+ "s_dongtai_eff,"
				+ "s_dongtai_sum,"
				+ "s_zan_eff,"
				+ "s_zan_sum,"
				+ "s_report_date) VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?)";
		Connection connection = DriverManager.getConnection(url, user, password);
		PreparedStatement pst = connection.prepareStatement(sql);
		
		Date now = new Date();
		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		now.setDate(now.getDate()-1);
		String report_date = date.format(now);
		
		
		pst.setInt(1, Integer.parseInt(re[0]));
		pst.setInt(2, Integer.parseInt(re[1]));
		pst.setInt(3, Integer.parseInt(re[2]));
		pst.setInt(4, Integer.parseInt(re[3]));
		pst.setInt(5, Integer.parseInt(re[4]));
		pst.setInt(6, Integer.parseInt(re[5]));
		pst.setInt(7, Integer.parseInt(re[6]));
		pst.setInt(8, Integer.parseInt(re[7]));
		pst.setInt(9, Integer.parseInt(re[8]));
		pst.setInt(10, Integer.parseInt(re[9]));
		pst.setString(11, report_date);
		pst.addBatch();
		try {
			pst.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	
//		Dataset<Row> sqlDF = session.sql("select "
//				+ "pagename s_pagename,"
//				+ "count(*) s_view_count,"
//				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
//				+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd HH:mm:ss') s_create_time "
//				+ "from pagedata "
//				+ "where pagename <> '' "
//				+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
//				+ "and logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
//				+ "and pagename is not null "
//				+ "group by pagename");
//		sqlDF.write().mode("append").jdbc(ConfigurationManager.getProperty(Constants.JDBC_URL2), "t_report_page_count",PropertiesUtil.getProperties());
	
		
	}
	
	
	
	
	
	
}

/*
 * 线上环境 val url =
 * "jdbc:mysql://192.168.0.120:3306/exiaoxin?characterEncoding=utf-8" val user =
 * "xxv2" val password = "xv2PassWD-321"
 * 
 * 测试环境 val url =
 * "jdbc:mysql://122.193.22.133:3310/exiaoxin?characterEncoding=utf-8" val user
 * = "root" val password = "test3pass"
 * 
 * 本地环境 val url = "jdbc:mysql://192.168.0.154:3306/lele?characterEncoding=utf-8"
 * val user = "root" val password = "root"
 * 
 */
