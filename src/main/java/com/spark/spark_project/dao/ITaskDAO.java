package com.spark.spark_project.dao;

import com.spark.spark_project.domain.Task;

/*
 * 任务管理DAO接口
 * */
public interface ITaskDAO {

	/*
	 * 根据主键查询任务
	 * */
	Task findById(long taskid);
}
