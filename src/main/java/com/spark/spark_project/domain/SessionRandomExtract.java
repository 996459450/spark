package com.spark.spark_project.domain;

public class SessionRandomExtract {

	private long taskid;
	private String sessionid;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;
	public long getTaskid() {
		return taskid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public String getStartTime() {
		return startTime;
	}
	public String getSearchKeywords() {
		return searchKeywords;
	}
	public String getClickCategoryIds() {
		return clickCategoryIds;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public void setSearchKeywords(String searchKeywords) {
		this.searchKeywords = searchKeywords;
	}
	public void setClickCategoryIds(String clickCategoryIds) {
		this.clickCategoryIds = clickCategoryIds;
	}
	
	
}
