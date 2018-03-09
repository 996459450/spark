package com.spark.spark_project.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IAdStatDAO;
import com.spark.spark_project.domain.AdStat;
import com.spark.spark_project.jdbc.JDBCHelper;
import com.spark.spark_project.model.AdStatQueryResult;

public class AdStatDAOImpl implements IAdStatDAO{

	public void updatebatch(List<AdStat> adStats) throws SQLException {
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		
		List<AdStat> insertAdStats = new ArrayList<AdStat>();
		List<AdStat> updateAdStat = new ArrayList<AdStat>();
		
		String selectSQL = "select count(*) from ad_stat"
				+ " where date=? "
				+ "and provice =? "
				+ "and city=? "
				+ "and ad_id=?";
		
		for(AdStat adStat : adStats) {
			final AdStatQueryResult queryResult = new AdStatQueryResult();
			Object[] params = new Object[] {
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid()
			};
			jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallback() {
				
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						queryResult.setCount(count);
					}
				}
			});
			
			int count = queryResult.getCount();
			if(count > 0) {
				updateAdStat.add(adStat);
			}else {
				insertAdStats.add(adStat);
			}
		}
		String insertSQL = "insert into ad_stat values(?,?,?,?,?)";
		List<Object[]> insertparamsList = new ArrayList<Object[]>();
		for(AdStat adStat : insertAdStats) {
			Object[] params = new Object[] {adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid(),
					adStat.getClickCount()};
			insertparamsList.add(params);
		}
		jdbcHelper.executebatch(insertSQL, insertparamsList);
		
		String updateSQL = "update ad_stat set click_count=?"
				+ " from ad_stat "
				+ " where date=?"
				+ " and provice =?"
				+ " and city=?"
				+ " and ad_id=?";
		List<Object[]> updateparamsList = new ArrayList<Object[]>();
		for(AdStat adStat : updateAdStat) {
			Object[] params = new Object[] {adStat.getClickCount(),
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid()};
			updateparamsList.add(params);
		}
		jdbcHelper.executebatch(updateSQL, updateparamsList);
	}

}
