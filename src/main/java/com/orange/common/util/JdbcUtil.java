package com.orange.common.util;

import java.util.Properties;

public class JdbcUtil {

	  public static Properties JdbcCon(){
		 Properties prop =  new Properties();
		 prop.put("user", "root");
		 prop.put("password", "test3pass");
		return prop;
	 }
	        
	    
}
