package com.spark.spark_project.dao;

import java.util.List;

import com.spark.spark_project.domain.AdBlicklist;

/*
 * 广告黑名单DAO接口
 * */
public interface IAdBlicklistDAO {

	/*
	 * 批量插入广告黑名单用户
	 * */
	void insertBatch(List<AdBlicklist> adBlicklists);
	
	/*
	 * 查询所有广告黑名单用户
	 * */
	List<AdBlicklist> findAll();
}
