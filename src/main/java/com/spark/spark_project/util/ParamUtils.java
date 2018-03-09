package com.spark.spark_project.util;

import com.alibaba.fastjson.JSONObject;
import com.spark.spark_project.conf.ConfigurationManage;
import com.spark.spark_project.constants.Canstants;

public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 * */
	public static Long getTaskIdFromArgs(String[] args,String taskType) {
		boolean local = ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
		if(local) {
			return ConfigurationManage.getLong(taskType);
		}else {
			try {
				if(args != null && args.length>0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	/**
	 * 从json对象中提取参数
	 * @param jsonobject
	 * @return 参数
	 * */
	public static String getParam(JSONObject jsonObject,String field) {
		JSONObject object = jsonObject.getJSONObject(field);
		if(object != null && object.size()>0) {
			return object.getString("0");
		}
		return null;
	}
}
