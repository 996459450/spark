package com.spark.spark_project.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 品类二次排序key
 * 封装进行排序算法需要的几个字段：点击次数，下单次数，支付次数
 * 实现ordered接口，以及serializable接口
 * */

public class CategorySortKey implements Ordered<CategorySortKey>,Serializable {

	private static final long serialVersionUID = 1L;

	private long clickcount;
	private long ordercount;
	private long paycount;
	
	public long getClickcount() {
		return clickcount;
	}

	public void setClickcount(long clickcount) {
		this.clickcount = clickcount;
	}

	public long getOrdercount() {
		return ordercount;
	}

	public void setOrdercount(long ordercount) {
		this.ordercount = ordercount;
	}

	public long getPaycount() {
		return paycount;
	}

	public void setPaycount(long paycount) {
		this.paycount = paycount;
	}

	public CategorySortKey(long clickcount,long ordercount,long paycount) {

		this.clickcount = clickcount;
		this.ordercount = ordercount;
		this.paycount = paycount;
	}
	
	public boolean $greater(CategorySortKey other) {

		if(clickcount > other.getClickcount()) {
			return true;
		}else if (clickcount==other.getClickcount() && ordercount>other.getOrdercount()) {
			return true;
		}else if (clickcount == other.getClickcount() && ordercount==other.getClickcount() && paycount>=other.getPaycount()) {
			return true;
		}
		return false;
	}

	public boolean $greater$eq(CategorySortKey other) {

		if($greater(other)) {
			return true;
		}else if(clickcount == other.getClickcount() && ordercount ==other.getOrdercount()&&paycount==other.getPaycount()) {
			return true;
		}
		return false;
	}

	public boolean $less(CategorySortKey other) {

		if(clickcount< other.getClickcount()) {
			return true;
		}else if(clickcount == other.getClickcount()&& ordercount <other.getOrdercount()) {
			return true;
		}else if(clickcount==other.getClickcount() && ordercount==other.getOrdercount()&& paycount<other.getPaycount()) {
			return true;
		}
		return false;
	}

	public boolean $less$eq(CategorySortKey other) {

		if($less(other)) {
			return true;
		}else if(clickcount ==other.getClickcount()
				&& ordercount==other.getOrdercount()&&
				paycount==other.getPaycount()) {
			return true;
		}
		return false;
	}

	public int compare(CategorySortKey other) {

		if(clickcount-other.getClickcount()!=0) {
			return (int) (clickcount-other.getClickcount());
		}else if(ordercount-other.getOrdercount()!=0) {
			return (int)(ordercount-other.getOrdercount());
		}else if(paycount-other.getPaycount()!=0){
			return (int) (paycount-other.getPaycount());
		}
		return 0;
	}

	public int compareTo(CategorySortKey other) {

		if(clickcount-other.getClickcount()!=0) {
			return (int) (clickcount-other.getClickcount());
		}else if(ordercount-other.getOrdercount()!=0) {
			return (int)(ordercount-other.getOrdercount());
		}else if(paycount-other.getPaycount()!=0){
			return (int) (paycount-other.getPaycount());
		}
		return 0;
	}

}
