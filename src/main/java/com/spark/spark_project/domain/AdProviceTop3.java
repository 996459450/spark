package com.spark.spark_project.domain;

/*
 * 各省top3热门广告
 * */
public class AdProviceTop3 {

	private String date;
	private String provice;
	private long adid;
	private long clickCount;
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getProvice() {
		return provice;
	}
	public void setProvice(String provice) {
		this.provice = provice;
	}
	public long getAdid() {
		return adid;
	}
	public void setAdid(long adid) {
		this.adid = adid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
	
}
