package com.spark.spark_project.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IAdBlicklistDAO;
import com.spark.spark_project.domain.AdBlicklist;
import com.spark.spark_project.jdbc.JDBCHelper;

public class AdBlacklistDAOImpl implements IAdBlicklistDAO{

	/**
	 * 批量插入广告黑名单用户
	 * */
	public void insertBatch(List<AdBlicklist> adBlicklists) {
		String sql = "insert into ad_blacklist values(?)";
		
		List<Object []> paramsList = new ArrayList<Object[]>();
		for (AdBlicklist adBlicklist : adBlicklists) {
			Object[] params = new Object[] {adBlicklist.getUserid()};
			paramsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		try {
			jdbcHelper.executebatch(sql, paramsList);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * 查询所有广告黑名单用户
	 * */
	public List<AdBlicklist> findAll() {
		String sql = "select * from ad_blacklist";
		final List<AdBlicklist> adBlacklists = new ArrayList<AdBlicklist>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {

				while(rs.next()) {}
				long userid = Long.parseLong(String.valueOf(rs.getInt(1)));
				
				AdBlicklist adBlicklist = new AdBlicklist();
				adBlicklist.setUserid(userid);
				
				adBlacklists.add(adBlicklist);
				
			}
		});
		
		return adBlacklists;
	}

}
