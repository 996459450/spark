package com.spark.spark_project.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IAreaTop3ProductDAO;
import com.spark.spark_project.domain.AreaTop3Product;
import com.spark.spark_project.jdbc.JDBCHelper;

public class AreaTop3ProdcutDAOImpl implements IAreaTop3ProductDAO{

	public void insertBatch(List<AreaTop3Product> areaTop3Products) throws SQLException {
		String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(AreaTop3Product aProduct : areaTop3Products) {
			Object[] params = new Object[8];
			
			params[0] = aProduct.getTaskid();
			params[1] = aProduct.getArea();
			params[2] = aProduct.getAreaLevel();
			params[3] = aProduct.getProductid();
			params[4] = aProduct.getCityInfos();
			params[5] = aProduct.getClickCount();
			params[6] = aProduct.getProductName();
			params[7] = aProduct.getProductStatus();
			paramsList.add(params);
		}		
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executebatch(sql, paramsList);
	}

	

	
}
