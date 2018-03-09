package com.spark.spark_project.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.spark.spark_project.dao.IAdProvinceTop3DAO;
import com.spark.spark_project.domain.AdProviceTop3;
import com.spark.spark_project.jdbc.JDBCHelper;

public class AdProviceTop3DAOImpl implements IAdProvinceTop3DAO{

	public void updateBatch(List<AdProviceTop3> adProviceTop3s) throws Exception {
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		List<String > dateProvices = new ArrayList<String>();
		for(AdProviceTop3 adProviceTop3 : adProviceTop3s) {
			String date = adProviceTop3.getDate();
			String provice = adProviceTop3.getProvice();
			String key = date+"_"+provice;
			
			if(!dateProvices.contains(key)) {
				dateProvices.add(key);
			}
		}
		
		String deleteSQl = "delete from ad-provice_top3 where date=? and provice=?";
		List<Object[]> deleteParamsList = new ArrayList<Object[]>();
		for(String dateProvice : dateProvices) {
			String[] dataParviceSplited = dateProvice.split("_");
			String date = dataParviceSplited[0];
			String provice = dataParviceSplited[1];
			
			Object[] params = new Object[] {date,provice};
			deleteParamsList.add(params);
		}
		
			jdbcHelper.executebatch(deleteSQl, deleteParamsList);
			String insertSql = "insert into ad_provice_top3 values(?,?,?,?)";
			List<Object[]> insterParamsList = new ArrayList<Object[]>();
			for(AdProviceTop3 adProviceTop3 : adProviceTop3s) {
				Object[] params = new Object[] {adProviceTop3.getDate(),adProviceTop3.getProvice(),
						adProviceTop3.getAdid(),adProviceTop3.getClickCount()};
				insterParamsList.add(params);
			}
			jdbcHelper.executebatch(insertSql, insterParamsList);
		
		
	}

	
}
