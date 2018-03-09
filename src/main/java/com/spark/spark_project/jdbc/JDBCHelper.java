package com.spark.spark_project.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.spark.spark_project.conf.ConfigurationManage;
import com.spark.spark_project.constants.Canstants;

public class JDBCHelper {

	static {
		try {
			String driver =ConfigurationManage.getProperties(Canstants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static JDBCHelper install =null;
	public static JDBCHelper getInstall() {
		if(install ==null) {
			synchronized (JDBCHelper.class) {
				install = new JDBCHelper();
			}
		}
		return install;
	}
	
	private LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	private JDBCHelper() {
		int datasourceSize = ConfigurationManage.getInteger(Canstants.JDBC_DATASOURCE_SIZE);
		for(int i=0;i< datasourceSize ;i++) {
			boolean local= ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
			String url ="";
			String user = "";
			String password ="";
			
			if(local) {
				url = ConfigurationManage.getProperties(Canstants.JDBC_URL);
				user = ConfigurationManage.getProperties(Canstants.JDBC_USER);
				password = ConfigurationManage.getProperties(Canstants.JDBC_PASSWORD);
			}else {
				url = ConfigurationManage.getProperties(Canstants.JDBC_URL_PROD);
				user = ConfigurationManage.getProperties(Canstants.JDBC_USER_PROD);
				password = ConfigurationManage.getProperties(Canstants.JDBC_PASSWORD_PROD);
			}
			
			try {
				java.sql.Connection conn = DriverManager.getConnection(url, user, password);
				datasource.push((Connection) conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public synchronized Connection getConnection() {
		
		while(datasource.size() ==0) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}
	
	public int executeUpdate(String sql,Object[] params) {
		int rtn =0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			pstmt= conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i=0;i< params.length;i++) {
					pstmt.setObject(i+1, params[i]);
				}
			}
			
			rtn = pstmt.executeUpdate();
			conn.commit();
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(conn != null) {
				datasource.push(conn);
			}
		}
		return rtn;
	}
	
	public void executeQuery(String sql,Object[] params,QueryCallback queryCallback) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			
			if(params!=null && params.length > 0) {
				for(int i=0;i < params.length;i++) {
					pstmt.setObject(i+1, params[i]);
				}
			}
			
			rs = pstmt.executeQuery();
			queryCallback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int[] executebatch(String sql,List<Object[]> paramsList) throws SQLException {
		int[] rnt = null;
		Connection conn = null;
		PreparedStatement  pstmt= null;
		
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			if(paramsList != null && paramsList.size()>0) {
				for(Object[] params : paramsList) {
					for(int i=0;i<params.length;i++) {
						pstmt.setObject(i+1, params[i]);
					}
					pstmt.addBatch();
				}
			}
			rnt = pstmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(conn !=null) {
				datasource.push(conn);
			}
		}
		return rnt;
	}
	
	public static interface QueryCallback{
		
		void process(ResultSet rs)throws Exception;
	}

}
