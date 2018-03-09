package com.spark.spark_project.dao.impl;

import com.spark.spark_project.dao.ITop10CategoryDAO;
import com.spark.spark_project.domain.Top10Category;
import com.spark.spark_project.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO{

	public void insert(Top10Category category) {

		String sql = "insert into top10_category values(?,?,?,?,?)";
		
		Object[] params = new Object[] {category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeUpdate(sql, params);
	}

}
