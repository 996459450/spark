package com.spark.spark_project.dao.impl;

import com.spark.spark_project.dao.IPageSplitConverRateDAO;
import com.spark.spark_project.domain.PageSplitConvertRate;
import com.spark.spark_project.jdbc.JDBCHelper;

public class PageSplitConvertRateDAOImpl implements IPageSplitConverRateDAO{

	public void inser(PageSplitConvertRate pageSplitConvertRate) {
		String sql ="insert into apge_split_convert_rate values(?,?)";
		Object[] params = new Object[] {pageSplitConvertRate.getTaskid(),
				pageSplitConvertRate.getConverRate()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeUpdate(sql, params);
	}

}
