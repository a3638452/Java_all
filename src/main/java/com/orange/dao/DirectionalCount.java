package com.orange.dao;
import org.apache.spark.sql.SparkSession;

public class DirectionalCount {
	
	
		public void getPlaceCount (SparkSession session){
			

			session.sql("use sdkdata");
			session.sql("select "
					+ "userid f_user_id,"
					+ "province s_province_name,"
					+ "city s_city_name,"
					+ "area s_area_name,"
					+ "streetarea s_streetarea,"
					+ "lng s_lng,"
					+ "lat s_lat,"
					+ "count(*) s_login_count,"
					+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
					+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
					+ "from logindata "
					+ "where "
					+ "province <> '' "
					+ "and city <> '' "
					+ "and area <> '' "
					+ "and province <> 'null' "
					+ "and city <> 'null' "
					+ "and area <> 'null' "
					+ "and province is not null "
					+ "and city is not null "
					+ "and area is not null "
					+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
					+ "and "
					+ "logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
					+ "group by "
					+ "userid,province,city,area,streetarea,lng,lat ").createOrReplaceTempView("t_1");
			session.sql("insert into t_user_place select "
					+ "f_user_id,"
					+ "s_province_name,"
					+ "s_city_name,"
					+ "s_area_name,"
					+ "s_streetarea,"
					+ "s_lng,"
					+ "s_lat,"
					+ "s_login_count,"
					+ "s_report_date,"
					+ "s_create_time "
					+ "from t_1");
			
			
		}
		
		
		
		public void getTimeTable(SparkSession session){
			session.sql("use sdkdata");
			session.sql("select "
					+ "userid f_user_id,"
					+ "province s_province_name,"
					+ "city s_city_name,"
					+ "area s_area_name,"
					+ "streetarea s_streetarea,"
					+ "lng s_lng,"
					+ "lat s_lat,"
					+ "substring_index(logintime,':',1) s_time_section,"
					+ "count(*) s_login_count,"
					+ "from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd') s_report_date,"
					+ "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') s_create_time "
					+ "from logindata "
					+ "where "
					+ "province <> '' "
					+ "and city <> '' "
					+ "and area <> '' "
					+ "and province <> 'null' "
					+ "and city <> 'null' "
					+ "and area <> 'null' "
					+ "and province is not null "
					+ "and city is not null "
					+ "and area is not null "
					+ "and logintime>from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd 00:00:00') "
					+ "and "
					+ "logintime<from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') "
					+ "group by "
					+ "userid,province,city,area,streetarea,lng,lat,substring_index(logintime,':',1) ").createOrReplaceTempView("t_1");
			session.sql("insert into t_user_timetable select "
					+ "f_user_id,"
					+ "s_province_name,"
					+ "s_city_name,"
					+ "s_area_name,"
					+ "s_streetarea,"
					+ "s_lng,"
					+ "s_lat,"
					+ "s_time_section,"
					+ "s_login_count,"
					+ "s_report_date,"
					+ "s_create_time "
					+ "from t_1");
			}
		
}


/*
 * 			Calendar ca = Calendar.getInstance();
			Date ti = new Date();//早时间
			Date ti2 = new Date();//晚时间
			SimpleDateFormat getdate = new SimpleDateFormat("yyyy-MM-dd");
			ti.setDate(ti.getDate()-1);
		
				ti.setDate(ti.getDate()-1);
				ti2.setDate(ti2.getDate()-1);
				String dt = getdate.format(ti);
				String dt2 = getdate.format(ti2);
				
				//					+ dt + "' s_report_date,"
				//					+ "logintime > '" + dt + " 00:00:00' "
//					+ "and "
//					+ "logintime < '" + dt2 + " 00:00:00' "
				 
				 //					+ dt + "' s_report_date,"

//					+ "logintime > '"+ dt +" 00:00:00' "
//					+ "and "
//					+ "logintime < '" + dt2 + " 00:00:00' "


							Calendar ca = Calendar.getInstance();
			Date ti = new Date();//早时间
			Date ti2 = new Date();//晚时间
			SimpleDateFormat getdate = new SimpleDateFormat("yyyy-MM-dd");
			ti.setDate(ti.getDate()-1);

				ti.setDate(ti.getDate()-1);
				ti2.setDate(ti2.getDate()-1);
				String dt = getdate.format(ti);
				String dt2 = getdate.format(ti2);
			
			*/
