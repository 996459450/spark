package com.spark.spark_project.dao;

import java.sql.SQLException;
import java.util.List;

import com.spark.spark_project.domain.AdStat;

public interface IAdStatDAO {

	void updatebatch(List<AdStat> adStats) throws SQLException;
}
