package com.spark.spark_project.dao;

import com.spark.spark_project.domain.SessionAggrStat;

/*
 * session 聚合统计模块DAO接口
 * */
public interface ISessionAggrStatDAO {

	/*
	 * 插入Session 聚合统计结果
	 * */
	void insert(SessionAggrStat sessionAggrStat);
}
