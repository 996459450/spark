package com.spark.spark_project.spark.product;

import java.util.Random;

import org.apache.spark.sql.api.java.UDF2;

public class RandomPrefixUDF implements UDF2<String	, Integer, String>{

	private static final long serialVersionUID = 1L;

	public String call(String val, Integer num) throws Exception {
		Random random = new Random();
		int randnum = random.nextInt(10);
		return randnum+"_"+val;
	}

}
