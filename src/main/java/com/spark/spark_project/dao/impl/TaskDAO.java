package com.spark.spark_project.dao.impl;

import java.sql.ResultSet;

import com.spark.spark_project.dao.ITaskDAO;
import com.spark.spark_project.domain.Task;
import com.spark.spark_project.jdbc.JDBCHelper;

public class TaskDAO implements ITaskDAO{

	public Task findById(long taskid) {
		final Task task = new Task();
		String sql="select * from task where task_id=?";
		Object[] params = new Object[] {taskid};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstall();
		jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {

				if(rs.next()) {
					long taskid = rs.getLong(1);
					String taskName=  rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finshTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskparams = rs.getString(8);
					
					task.setTaskid(taskid);
					task.setTaskName(taskName);
					task.setCreateTime(createTime);
					task.setStartTime(startTime);
					task.setFinshTime(finshTime);
					task.setTaskType(taskType);
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskparams);
				}
			}
		});
		return task;
	}

}
