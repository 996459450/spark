package com.spark.spark_project.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;

public class GetJsonObjectUDF implements UDF2<String, String, String>{

	private static final long serialVersionUID = 7696610434810983556L;

	public String call(String json, String field) throws Exception {
		try {
			JSONObject jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
