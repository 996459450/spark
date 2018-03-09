package com.spark.spark_project.dao;

import java.sql.SQLException;
import java.util.List;

import com.spark.spark_project.domain.AreaTop3Product;

/*
 * 各区域top3热门商品DAO接口
 * */
public interface IAreaTop3ProductDAO {

	void insertBatch(List<AreaTop3Product> areaTop3Products) throws SQLException;
}
