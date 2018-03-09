package com.spark.spark_project.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IAdUserClickCountDAO;
import com.spark.spark_project.domain.AdUserClickCount;
import com.spark.spark_project.jdbc.JDBCHelper;
import com.spark.spark_project.jdbc.JDBCHelper.QueryCallback;
import com.spark.spark_project.model.AdUserClickQueryResult;

public class AdUserClickCountDAOimpl implements IAdUserClickCountDAO{

	public void updateBatch(List<AdUserClickCount> adUserClickCounts) throws Exception {
		JDBCHelper jdbcHelper =JDBCHelper.getInstall();
		
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();
		
		String selectSQl = "select count(*) from ad_user_click_count"
				+ " where date=? and user=? and ad_id=?";
		Object[] selectParams =null;
		for(AdUserClickCount adUserClickCount : adUserClickCounts) {
			final AdUserClickQueryResult queryResult = new AdUserClickQueryResult();
			selectParams = new Object[] {adUserClickCount.getDate(),
					adUserClickCount.getAdid(),
					adUserClickCount.getAdid()};
			jdbcHelper.executeQuery(selectSQl, selectParams, new QueryCallback() {
				
				public void process(ResultSet rs) throws Exception {

					if(rs.next()) {
						int count= rs.getInt(1);
						queryResult.setCount(count);
					}
				}
			});
			
			int count = queryResult.getCount();
			if(count > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			}else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
			String inserSQL = "insert into ad_user_click_count values(?,?,?,?,?)";
			List<Object[]> insertParamslist = new ArrayList<Object[]>();
			for(AdUserClickCount adUserClickCount2 : insertAdUserClickCounts) {
				Object[] insertParams = new Object[] {adUserClickCount2.getDate(),
						adUserClickCount2.getUserid(),
						adUserClickCount2.getAdid(),
						adUserClickCount2.getCliclCount()};
				insertParamslist.add(insertParams);
			}
			jdbcHelper.executebatch(inserSQL, insertParamslist);
		}
	}

	public int findClickCountByMultiKey(String date, long userid, long adid) {
		String sql = "select click_count "
				+ "from ad_user_click_count "
				+ "where date=? "
				+ "and user_id=? "
				+ "and ad_id=?";
		Object[] params = new Object[] {date,userid,adid};
		final AdUserClickQueryResult queryResult = new AdUserClickQueryResult();
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeQuery(sql, params, new QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {

				if(rs.next()) {
					int clickCount= rs.getInt(1);
					queryResult.setClickCount(clickCount);
				}
			}
		});
		int clickCount = queryResult.getClickCount();
		return clickCount;
	}

}
