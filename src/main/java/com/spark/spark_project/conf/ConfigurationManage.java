package com.spark.spark_project.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManage {

	private static Properties prop = new Properties();
	
	static {
		try {
			InputStream in = ConfigurationManage.class.getClassLoader().getResourceAsStream("my.properties");
			prop.load(in);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public static String getProperties(String key) {
		return prop.getProperty(key);
	}
	
	public static Integer getInteger(String key) {
		String value = getProperties(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static boolean getBoolean(String key) {
		String value = getProperties(key);
		try {
			return Boolean.getBoolean(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public static Long getLong(String key) {
		String value = getProperties(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
}
