package com.spark.spark_project.dao;

import java.util.List;

import com.spark.spark_project.domain.AdProviceTop3;

/**
 * 各个省份top3热门广告DAO接口
 * */
public interface IAdProvinceTop3DAO {

	void updateBatch(List<AdProviceTop3> adProviceTop3s) throws Exception;
}
