package com.spark.spark_project.spark.product;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String, String>{

	private static final long serialVersionUID = 1L;

	public String call(String val) throws Exception {
		String[] valsplited= val.split("_");
		return valsplited[1];
	}

}
