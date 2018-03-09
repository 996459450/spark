package com.spark.spark_project.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.spark.spark_project.conf.ConfigurationManage;
import com.spark.spark_project.constants.Canstants;

public class SparkUtils {

	/**
	 * 根据当前是否本地测试配置决定
	 *如何设置当前的sparkconf的master 
	 * */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
		if(local) {
			conf.setMaster("local");
		}
	}
	
	/**
	 * 获取SQlContext
	 * 如果spark.local设置为true，那么就创建SQlContext；否则就创建HiveContext
	 * @param sc
	 * @return context
	 * */
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		}else {
			return new HiveContext(sc);
		}
	}
	/**
	 * 生成模拟数据
	 * 如果spark.local设置为true，则生成模拟数据，否则不生成
	 * @param sc
	 * @return sqlContext
	 * */
	public static void mockData(JavaSparkContext sc,SQLContext sqlContext) {
		boolean local = ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
		if(local) {
			mockData(sc, sqlContext);
		}
	}
	
	/**
	 * 获取指定日期范围内的用户行为数据RDD
	 * @param sqlContext
	 * @param taskParam
	 * @return 
	 * */
	public static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext,JSONObject taskRange){
		String enddate = ParamUtils.getParam(taskRange, Canstants.PARAM_END_DATE);
		String startDate = ParamUtils.getParam(taskRange, Canstants.PARAM_START_TIME);
		
		String sql = "select *"
				+ "from user_visit-action "
				+ "and date >='"+startDate+"'"
				+ " and date <='"+enddate+"'";
		
		DataFrame actionDF = sqlContext.sql(sql);
		
		return actionDF.javaRDD();
	}
}
