package com.spark.spark_project.spark.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class GroupConcatDistantctUDAF extends UserDefinedAggregateFunction{

	private static final long serialVersionUID = 1L;

	//指定输入数据的字段与类型
	private StructType inputSchame = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("city info", DataTypes.StringType, true)));
	//指定缓冲数据的字段与类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("buffercityinfo", DataTypes.StringType, true)));
	//
	private DataType dataType = DataTypes.StringType;
	//指定是否是确定性的
	private boolean determinitsic = true;
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return determinitsic;
	}

	@Override
	public Object evaluate(Row row) {
		return row.getString(0);
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	@Override
	public StructType inputSchema() {
		return inputSchame;
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);
		
		for(String cityInfo: bufferCityInfo2.split(",")) {
			if(!bufferCityInfo1.contains(cityInfo)) {
				if("".equals(bufferCityInfo1)) {
					bufferCityInfo1+=cityInfo;
				}else {
					bufferCityInfo1+=","+cityInfo;
				}
			}
		}
		buffer1.update(1, bufferCityInfo1);
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {

		String bufferCityInfo = buffer.getString(0);
		String cityinfo = input.getString(0);
		
		if(!bufferCityInfo.contains(cityinfo)) {
			if("".equals(bufferCityInfo)) {
				bufferCityInfo+=cityinfo;
			}else {
				bufferCityInfo+=","+cityinfo;
			}
		}
		buffer.update(1, bufferCityInfo);
	}

}
