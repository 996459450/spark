package com.spark.spark_project.domain;

public class Top10Session {

	private long taskid;
	private long categoryid;
	private String sessionid;
	private long clickCount;
	public long getTaskid() {
		return taskid;
	}
	public long getCategoryid() {
		return categoryid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
	
}
