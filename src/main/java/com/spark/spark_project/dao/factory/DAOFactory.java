package com.spark.spark_project.dao.factory;

import com.spark.spark_project.dao.IAdBlicklistDAO;
import com.spark.spark_project.dao.IAdProvinceTop3DAO;
import com.spark.spark_project.dao.IAdStatDAO;
import com.spark.spark_project.dao.IAdUserClickCountDAO;
import com.spark.spark_project.dao.IAreaTop3ProductDAO;
import com.spark.spark_project.dao.IPageSplitConverRateDAO;
import com.spark.spark_project.dao.ISessionAggrStatDAO;
import com.spark.spark_project.dao.ISessionDetailDAO;
import com.spark.spark_project.dao.ISessionRandomExtractDAO;
import com.spark.spark_project.dao.ITaskDAO;
import com.spark.spark_project.dao.ITop10CategoryDAO;
import com.spark.spark_project.dao.ITop10SessionDAO;
import com.spark.spark_project.dao.IaClickTrendDAO;
import com.spark.spark_project.dao.impl.AdBlacklistDAOImpl;
import com.spark.spark_project.dao.impl.AdClickTrendDAOImpl;
import com.spark.spark_project.dao.impl.AdProviceTop3DAOImpl;
import com.spark.spark_project.dao.impl.AdStatDAOImpl;
import com.spark.spark_project.dao.impl.AdUserClickCountDAOimpl;
import com.spark.spark_project.dao.impl.AreaTop3ProdcutDAOImpl;
import com.spark.spark_project.dao.impl.PageSplitConvertRateDAOImpl;
import com.spark.spark_project.dao.impl.SessionAggrStatDAOImpl;
import com.spark.spark_project.dao.impl.SessionDetailDAOImpl;
import com.spark.spark_project.dao.impl.SessionRandomExtractDAOImpl;
import com.spark.spark_project.dao.impl.TaskDAO;
import com.spark.spark_project.dao.impl.Top10CategoryDAOImpl;
import com.spark.spark_project.dao.impl.Top10SessionDAoImpl;

public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAO();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getISessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAoImpl();
	}
	
	public static IPageSplitConverRateDAO getPageConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProdcutDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOimpl();
	}
	
	public static IAdBlicklistDAO getAdBlicklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProviceTop3DAOImpl();
	}
	
	public static IaClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
