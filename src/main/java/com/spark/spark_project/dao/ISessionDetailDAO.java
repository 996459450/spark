package com.spark.spark_project.dao;

import java.sql.SQLException;
import java.util.List;

import com.spark.spark_project.domain.SessionDetail;

/*
 * Session明细DAO接口
 * */
public interface ISessionDetailDAO {

	/*
	 * 插入一条session明细数据
	 * */
	void insert(SessionDetail sessionDetail);
	
	/*
	 * 批量插入session明细数据
	 * */
	void insertBatch(List<SessionDetail> sessionDetails) throws SQLException;
}
