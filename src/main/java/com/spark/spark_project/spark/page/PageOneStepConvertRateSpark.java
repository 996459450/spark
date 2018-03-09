package com.spark.spark_project.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.spark.spark_project.constants.Canstants;
import com.spark.spark_project.dao.IPageSplitConverRateDAO;
import com.spark.spark_project.dao.ITaskDAO;
import com.spark.spark_project.dao.factory.DAOFactory;
import com.spark.spark_project.domain.PageSplitConvertRate;
import com.spark.spark_project.domain.Task;
import com.spark.spark_project.util.DateUtils;
import com.spark.spark_project.util.NumberUtils;
import com.spark.spark_project.util.ParamUtils;
import com.spark.spark_project.util.SparkUtils;

import scala.Tuple2;

public class PageOneStepConvertRateSpark {

	public static void main(String[] args) {
		//构造spark上下文
		SparkConf conf = new SparkConf().setAppName(Canstants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		//生成模拟数据
		SparkUtils.mockData(sc, sqlContext);
		
		//查询任务，获取任务的参数
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Canstants.SPARK_LOCAL_TASKID_PAGE);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() +": cannot find this task with id ["+taskid+"]");
			return ;
		}
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		//查询指定日期范围内的用户访问行为
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDataRange(sqlContext, taskParam);
		
		//对用户的访问行为数据做一个映射，将其映射为<sessionid,访问行为>
		JavaPairRDD<String,Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
		sessionid2actionRDD = sessionid2actionRDD.cache();
		
		//对<sessionid,访问行为>RDD,做一次groupbykey的操作
		//拿到每个sess对应的访问行为数据
		JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
		
		//每个session的单挑页面切片的生成，以及页面流的匹配算法
		JavaPairRDD<String, Integer> pageSplitRdd = generateAndMatchpageSplit(sc, sessionid2actionsRDD, taskParam);
		Map<String, Object> pageSplitPvMap = pageSplitRdd.countByKey();
		long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
		//计算目标页面流的各个页面切片的转换率
		Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);
		//持久化页面的切片转化率
		persistRateMap(taskid, convertRateMap);
		
	}
	/**
	 * 获取<sessionid,用户访问行为>格式的数据
	 * @param actionRDD 用户访问行为RDD
	 * @return <sessionid,用户访问行为>格式的数据
	 * */
	private static JavaPairRDD< String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD){
		
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(2);
				return new Tuple2<String, Row>(sessionid, row);
			}
		});
	}
	
	/**
	 * 页面切片生成与匹配算法
	 * @param sc
	 * @param sessionid2actionsRDD
	 * @param taskParam
	 * @return
	 * */
	private static JavaPairRDD<String, Integer> generateAndMatchpageSplit(JavaSparkContext sc,JavaPairRDD<String , Iterable<Row>> session2actionsRDD,JSONObject taskParam){
		
		String targetpageFlow = ParamUtils.getParam(taskParam, Canstants.PANAM_TARGET_PAGE_FLOW);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetpageFlow);
		return session2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				//定义返回list
				ArrayList<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				//获取到当前session访问行为的迭代器
				Iterator<Row> iterator = tuple._2.iterator();
				//获取使用者指定的页面流
				String[] targetPages = targetPageFlowBroadcast.value().split(",");
				
				ArrayList<Row> rows = new ArrayList<Row>();
				while (iterator.hasNext()) {
					rows.add(iterator.next());
				}
				
				Collections.sort(rows,new Comparator<Row>() {

					public int compare(Row o1, Row o2) {
						String actionTime1 = o1.getString(4);
						String actionTime2 = o2.getString(4);
						
						Date date1 = DateUtils.parseTime(actionTime1);
						Date date2 = DateUtils.parseTime(actionTime2);
						
						return (int)(date1.getTime() - date2.getTime());
					}
				});
				//页面切片的生成，以及页面流的匹配
				Long lastPageid= null;
				
				for(Row row:rows) {
					long pageid = row.getLong(3);
					if(lastPageid==null) {
						lastPageid = pageid;
						continue;
					}
					//生成一个页面切片
					
					String pagesplit = lastPageid+"_"+pageid;
					//对切片进行判断，是否在用户指定的页面流中
					for(int i=1;i<targetPages.length;i++) {
						String targetpageSplit = targetPages[i-1]+"_"+targetPages[i];
						if(pagesplit.equals(targetpageSplit)) {
							list.add(new Tuple2<String, Integer>(pagesplit, 1));
						}
					}
					lastPageid = pageid;
				}
				
				return list;
			}
		});
		
	}
	
	/**
	 * 获取页面中初始页面的pv
	 * @param taskParam
	 * @param sessionid2actionsRDD
	 * @return
	 * */
	private static long getStartPagePv(JSONObject taskParam,JavaPairRDD<String , Iterable<Row>> sessionid2actionsRDD) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Canstants.PANAM_TARGET_PAGE_FLOW);
		final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				ArrayList<Long> list = new ArrayList<Long>();
				Iterator<Row> iterator = tuple._2.iterator();
				while(iterator.hasNext()) {
					Row row = iterator.next();
					long pageid = row.getLong(3);
					if(pageid == startPageId) {
						list.add(pageid);
					}
				}
				return list;
			}
		});
		return startPageRDD.count();
	}
	
	/**
	 * 计算页面切片的转换率
	 * @param pageSplitPvMap 页面切片pv
	 * @param startPagePv其实页面pv
	 * @return
	 * */
	private static Map<String, Double> computePageSplitConvertRate(
			JSONObject taskParam,Map<String, Object> pageSplitPvMap,long startPagepv){
		Map<String, Double> convertRateMap = new HashMap<String , Double>();
		String[] targetPages = ParamUtils.getParam(taskParam, Canstants.PANAM_TARGET_PAGE_FLOW).split(",");
		long lastPageSplitPv = 0L;
		for(int i=1;i<targetPages.length;i++) {
			String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
			long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
			double convertRate = 0.0;
			
			if(i ==1) {
				convertRate = NumberUtils.formatDouble((double)targetPageSplitPv/(double) startPagepv,2);
			}else {
				convertRate = NumberUtils.formatDouble((double) targetPageSplitPv/(double) lastPageSplitPv, 2);
			}
			
			convertRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPv = targetPageSplitPv;
		}
		return convertRateMap;
	}
	
	/**
	 * 持久化转换率
	 * @param convertRateMap
	 * */
	private static void persistRateMap(long taskid ,Map<String, Double> convertRateMap) {
		StringBuffer buffer = new StringBuffer("");
		for(Map.Entry<String , Double> convertRateEntry:convertRateMap.entrySet()) {
			String pageSplit = convertRateEntry.getKey();
			double convertRate = convertRateEntry.getValue();
			buffer.append(pageSplit+"="+convertRate+"|");
		}
		
		String convertRate = buffer.toString();
		convertRate = convertRate.substring(0,convertRate.length()-1);
		
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskid(taskid);
		pageSplitConvertRate.setConverRate(convertRate);
		
		IPageSplitConverRateDAO pageSplitConverRateDAO = DAOFactory.getPageConvertRateDAO();
		pageSplitConverRateDAO.inser(pageSplitConvertRate);
	}
}
