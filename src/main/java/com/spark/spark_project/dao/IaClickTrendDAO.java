package com.spark.spark_project.dao;

import java.util.List;

import com.spark.spark_project.domain.AdClickTrend;

/*
 * 广告点击趋势DAO接口
 * */
public interface IaClickTrendDAO {

	void updateBatch(List<AdClickTrend> adClickTrends);
}
