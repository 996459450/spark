package com.spark.spark_project.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.ISessionDetailDAO;
import com.spark.spark_project.domain.SessionDetail;
import com.spark.spark_project.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetailDAO{

	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		
		Object[] params= new Object[] {sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActiontime(),
				sessionDetail.getSearchKeyWord(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPageid()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeUpdate(sql, params);
	}

	public void insertBatch(List<SessionDetail> sessionDetails) throws SQLException {

		String sql = "";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(SessionDetail sessionDetail : sessionDetails) {
			Object[] params = new Object[] {sessionDetail.getTaskid(),
					sessionDetail.getSessionid(),
					sessionDetail.getPageid(),
					sessionDetail.getActiontime(),
					sessionDetail.getSearchKeyWord(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()};
			paramsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executebatch(sql, paramsList);
	}

}
