package com.spark.spark_project.domain;

public class Top10Category {

	private long taskid;
	private long categoryid;
	private long clickCount;
	private long orderCount;
	private long payCount;
	public long getTaskid() {
		return taskid;
	}
	public long getCategoryid() {
		return categoryid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public long getOrderCount() {
		return orderCount;
	}
	public long getPayCount() {
		return payCount;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}
	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}
	
	
}
