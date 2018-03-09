package com.spark.spark_project.dao;

import com.spark.spark_project.domain.SessionRandomExtract;

/*
 * session 随机抽取模块DAO接口
 * */
public interface ISessionRandomExtractDAO {

	/*
	 * 插入随机抽取
	 * */
	void insert (SessionRandomExtract sessionRandomExtract);
}
