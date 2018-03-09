package com.spark.spark_project.domain;

public class SessionDetail {

	private long taskid;
	private long userid;
	private String sessionid;
	private long pageid;
	private String actiontime;
	private String searchKeyWord;
	private long clickCategoryId;
	private long clickProductId;
	private String orderCategoryIds;
	private String orderProductIds;
	private String payCategoryIds;
	private String payProductIds;
	public long getTaskid() {
		return taskid;
	}
	public long getUserid() {
		return userid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public long getPageid() {
		return pageid;
	}
	public String getActiontime() {
		return actiontime;
	}
	public String getSearchKeyWord() {
		return searchKeyWord;
	}
	public long getClickCategoryId() {
		return clickCategoryId;
	}
	public long getClickProductId() {
		return clickProductId;
	}
	public String getOrderCategoryIds() {
		return orderCategoryIds;
	}
	public String getOrderProductIds() {
		return orderProductIds;
	}
	public String getPayCategoryIds() {
		return payCategoryIds;
	}
	public String getPayProductIds() {
		return payProductIds;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public void setPageid(long pageid) {
		this.pageid = pageid;
	}
	public void setActiontime(String actiontime) {
		this.actiontime = actiontime;
	}
	public void setSearchKeyWord(String searchKeyWord) {
		this.searchKeyWord = searchKeyWord;
	}
	public void setClickCategoryId(long clickCategoryId) {
		this.clickCategoryId = clickCategoryId;
	}
	public void setClickProductId(long clickProductId) {
		this.clickProductId = clickProductId;
	}
	public void setOrderCategoryIds(String orderCategoryIds) {
		this.orderCategoryIds = orderCategoryIds;
	}
	public void setOrderProductIds(String orderProductIds) {
		this.orderProductIds = orderProductIds;
	}
	public void setPayCategoryIds(String payCategoryIds) {
		this.payCategoryIds = payCategoryIds;
	}
	public void setPayProductIds(String payProductIds) {
		this.payProductIds = payProductIds;
	}
	
	
}
