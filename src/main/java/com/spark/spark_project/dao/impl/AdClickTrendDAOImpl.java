package com.spark.spark_project.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IaClickTrendDAO;
import com.spark.spark_project.domain.AdClickTrend;
import com.spark.spark_project.jdbc.JDBCHelper;
import com.spark.spark_project.model.AdClickTrendQueryResult;


public class AdClickTrendDAOImpl implements IaClickTrendDAO{

	public void updateBatch(List<AdClickTrend> adClickTrends) {

		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		
		List<AdClickTrend> updateAdClickTrends = new ArrayList<AdClickTrend>();
		List<AdClickTrend> insertAdClickTrends = new ArrayList<AdClickTrend>();
		String selectSQl = "select count(*) "
				+ "from ad_click_trend" 
				+" where date=?"
				+" and hour=? "
				+ "and minute=?"
				+" and ad_id=?";
		
		for(AdClickTrend adClickTrend: adClickTrends) {
			final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();
			Object[] params = new Object[] {
					adClickTrend.getDate(),adClickTrend.getHour(),adClickTrend.getMinute(),adClickTrend.getAdid()
			};
			
			jdbcHelper.executeQuery(selectSQl, params, new JDBCHelper.QueryCallback() {
				
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						queryResult.setCount(count);
					}
				}
			});
			
			int count = queryResult.getCount();
			if(count > 0) {
				updateAdClickTrends.add(adClickTrend);
			}else {
				insertAdClickTrends.add(adClickTrend);
			}
		}
		
		String updateSQl = "update ad_click_trends set click_count=?"
				+" where date=? "
				+" and hour=? "
				+" and minute=? "
				+" and ad_id=?";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		
		for(AdClickTrend adClickTrend : updateAdClickTrends) {
			Object[] params = new Object[] {adClickTrend.getClickCount(),
					adClickTrend.getDate(),
					adClickTrend.getHour(),
					adClickTrend.getMinute(),
					adClickTrend.getAdid()};
			updateParamsList.add(params);
		}
		
try {
	jdbcHelper.executebatch(updateSQl, updateParamsList);
	String insertSql = "insert into ad_click_trend value(?,?,?,?,?)";
	List<Object[]> insertParamsList= new ArrayList<Object[]>();
	for(AdClickTrend adClickTrend: insertAdClickTrends) {
		Object[] params= new Object[] {adClickTrend.getDate(),
				adClickTrend.getHour(),
				adClickTrend.getMinute(),
				adClickTrend.getAdid(),
				adClickTrend.getAdid()};
		insertParamsList.add(params);
	}
	
	jdbcHelper.executebatch(insertSql, insertParamsList);
} catch (SQLException e) {
	e.printStackTrace();
}

		
		
	}

}
