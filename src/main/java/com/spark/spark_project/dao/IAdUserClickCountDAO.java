package com.spark.spark_project.dao;

import java.util.List;

import com.spark.spark_project.domain.AdUserClickCount;

/*
 * 用户广告点击量DAO接口
 * */
public interface IAdUserClickCountDAO {

	/*
	 * 批量更新用户广告点击量
	 * */
	void updateBatch(List<AdUserClickCount> adUserClickCounts) throws Exception;
	
	/*
	 * 根据多个key查询用户广告点击量
	 * */
	int findClickCountByMultiKey(String date,long userid,long adid);
}
