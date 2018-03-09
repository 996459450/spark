package com.spark.spark_project.dao.impl;

import com.spark.spark_project.dao.ISessionRandomExtractDAO;
import com.spark.spark_project.domain.SessionRandomExtract;
import com.spark.spark_project.jdbc.JDBCHelper;

public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
	public void insert(SessionRandomExtract sessionRandomExtract) {
		// TODO Auto-generated method stub
		String sql ="insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[] {sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeUpdate(sql, params);
	}

	
}
