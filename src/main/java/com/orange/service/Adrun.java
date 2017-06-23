package com.orange.service;

import org.apache.spark.sql.SparkSession;
import com.orange.common.util.SessionOfSpark;
import com.orange.dao.DirectionalCount;

public class Adrun {
	public static void main(String[] args) {
		SparkSession session = SessionOfSpark.getSparkSQLSession("Ad-user_place+timetable");
		new DirectionalCount().getPlaceCount(session);
		new DirectionalCount().getTimeTable(session);
		session.stop();
	}

}