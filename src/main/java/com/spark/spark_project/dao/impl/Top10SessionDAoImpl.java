package com.spark.spark_project.dao.impl;

import com.spark.spark_project.dao.ITop10SessionDAO;
import com.spark.spark_project.domain.Top10Session;
import com.spark.spark_project.jdbc.JDBCHelper;

public class Top10SessionDAoImpl implements ITop10SessionDAO{

	public void insert(Top10Session top10Session) {

		String sql ="insert into top10_session values(?,?,?,?,?)";
		Object[] params = new Object[] {top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeUpdate(sql, params);
	}

}
