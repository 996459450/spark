package com.spark.spark_project.spark.ad;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;
import com.spark.spark_project.conf.ConfigurationManage;
import com.spark.spark_project.constants.Canstants;
import com.spark.spark_project.dao.IAdBlicklistDAO;
import com.spark.spark_project.dao.IAdProvinceTop3DAO;
import com.spark.spark_project.dao.IAdStatDAO;
import com.spark.spark_project.dao.IAdUserClickCountDAO;
import com.spark.spark_project.dao.IaClickTrendDAO;
import com.spark.spark_project.dao.factory.DAOFactory;
import com.spark.spark_project.domain.AdBlicklist;
import com.spark.spark_project.domain.AdClickTrend;
import com.spark.spark_project.domain.AdProviceTop3;
import com.spark.spark_project.domain.AdStat;
import com.spark.spark_project.domain.AdUserClickCount;
import com.spark.spark_project.util.DateUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/*
 * 广告点击流量实时统计Spark任务
 * */
public class AdClickRealTimestatSpark {

	public static void main(String[] args) {
		// 构建Sparking Streaming 的上下文
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("adClickRealTimeStatSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		jssc.checkpoint("/data");
		Map<String , String > kafkaPrarms = new HashMap<String, String>();
		kafkaPrarms.put("metadata.broker.list", ConfigurationManage.getProperties(Canstants.KAFKA_METADATA_BROKER_LIST));
		String kafkatopics = ConfigurationManage.getProperties(Canstants.KAFKA_TOPICS);
		String[] kafkatopicSplited = kafkatopics.split(",");
		
		Set<String > topics = new HashSet<String>();
		for(String kafkatopic: kafkatopicSplited) {
			topics.add(kafkatopic);
		}
		//从kafka中读取数据
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaPrarms, topics);
		//根据动态黑名单进行数据过滤
		JavaPairDStream<String, String> filteredRealTimeLogDStream = 
				filterByBlockList(adRealTimeLogDStream);
		//生成动态黑名单
		gebarateDynamicBlicklist(filteredRealTimeLogDStream);
		JavaPairDStream<String, Long> adRealTimeStatDStream = calCulateRealTimeStat(filteredRealTimeLogDStream);
		calculateProvinceTop3Ad(adRealTimeStatDStream);

		calculateAdClickCountByWindow(adRealTimeLogDStream);
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	
	private static JavaPairDStream<String, String> filterByBlockList(JavaPairInputDStream<String, String> adRealTimeLogDstream){
		JavaPairDStream< String, String> filteredAdRealTimeLogDStream = adRealTimeLogDstream.transformToPair(
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

					private static final long serialVersionUID = 1L;

			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
				IAdBlicklistDAO iAdBlicklistDAO = DAOFactory.getAdBlicklistDAO();
				//从mysql中查找所有的黑名单用户
				List<AdBlicklist> adBlicklists = iAdBlicklistDAO.findAll();
				ArrayList<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
				for(AdBlicklist adBlicklist : adBlicklists) {
					tuples.add(new Tuple2<Long, Boolean>(adBlicklist.getUserid(), true));
				}
				JavaSparkContext sc = new JavaSparkContext(rdd.context());
				JavaPairRDD<Long, Boolean> blicklistRDD = sc.parallelizePairs(tuples);
				JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
						String log = tuple._2;
						String[] logSplited = log.split(" ");
						long userid = Long.valueOf(logSplited[3]);
						return new Tuple2<Long, Tuple2<String,String>>(userid, tuple);
					}
				});
				sc.close();
				//原始日志中的RDD与黑名单中的RDD做join操作，在黑名单中没有的用户将会被过滤掉
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blicklistRDD);
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
						new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {

							private static final long serialVersionUID = 1L;

							public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
									throws Exception {
								Optional<Boolean> optional = tuple._2._2;
								if(optional.isPresent() && optional.get()) {
									return false;
								}
								return true;
							}
						});
				JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String	, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
							throws Exception {
						
						return tuple._2._1;
					}
				});
				
				
				return resultRDD;
			}
			
		});
		
		return filteredAdRealTimeLogDStream;
	}
	
	private static void gebarateDynamicBlicklist(JavaPairDStream<String, String> filteredRdRealTimeLogDstream) {
		JavaPairDStream<String, Long> dailyUserAdclickDStream = filteredRdRealTimeLogDstream.mapToPair(
				new PairFunction<Tuple2<String,String>, String,Long>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						String log = tuple._2;
						String[] logSplit = log.split(" ");
						String timestamp = logSplit[0];
						Date date = new Date(Long.valueOf(timestamp));
						String datakey = DateUtils.formatDateKey(date);
						long userid = Long.valueOf(logSplit[3]);
						long adid = Long.valueOf(logSplit[4]);
						
						String key = datakey+"_"+userid+"_"+adid;
						return new Tuple2<String, Long>(key, 1L);
					}
				});
		
		JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdclickDStream.reduceByKey(
				new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1+v2;
					}
				});
		dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						ArrayList<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
						while(iterator.hasNext()) {
							Tuple2<String,Long> tuple = iterator.next();
							String[] keySplited = tuple._1.split(" ");
							String date = DateUtils.formateDate(DateUtils.parseDateKey(keySplited[0]));
							long userid = Long.valueOf(keySplited[1]);
							long adid = Long.valueOf(keySplited[2]);
							long clickCount = tuple._2;
							
							AdUserClickCount adUserClickCount = new AdUserClickCount();
							adUserClickCount.setDate(date);
							adUserClickCount.setUserid(userid);
							adUserClickCount.setAdid(adid);
							adUserClickCount.setCliclCount(clickCount);
							adUserClickCounts.add(adUserClickCount);
						}
						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						adUserClickCountDAO.updateBatch(adUserClickCounts);
					}
				});
				return null;
			}
		});
		JavaPairDStream<String, Long> blacllistuseridDStream = dailyUserAdClickCountDStream.filter(
				new Function<Tuple2<String,Long>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Long> tuple) throws Exception {
						String key = tuple._1;
						String[] keySplit = key.split(" ");
						String date = DateUtils.formateDate(DateUtils.parseDateKey(keySplit[0]));
						long userid = Long.valueOf(keySplit[1]);
						long adid = Long.valueOf(keySplit[2]);
						
						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						int clickcount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);
						if(clickcount >= 100) {
							return true;
						}
						return false;
					}
				});
		
		JavaDStream<Long> distinctblickUseridDStream = blacllistuseridDStream.map(new Function<Tuple2<String,Long>, Long>() {
			private static final long serialVersionUID = 1L;

			public Long call(Tuple2<String, Long> tuple) throws Exception {
				String key = tuple._1;
				String[] keySplited = key.split(" ");
				Long userid = Long.valueOf(keySplited[1]);
				return userid;
			}
		});
		
		JavaDStream<Long> distinctblicklistUserifDStream = distinctblickUseridDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {

			private static final long serialVersionUID = 1L;

			public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
				return rdd.distinct();
			}
		});
		distinctblicklistUserifDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Long> iterator) throws Exception {
						ArrayList<AdBlicklist> adBlicklists = new ArrayList<AdBlicklist>();
						while(iterator.hasNext()) {
							long userid= iterator.next();
							AdBlicklist adBlicklist = new AdBlicklist();
							adBlicklist.setUserid(userid);
							adBlicklists.add(adBlicklist);
						}
						IAdBlicklistDAO adBlicklistDAO = DAOFactory.getAdBlicklistDAO();
						adBlicklistDAO.insertBatch(adBlicklists);
					}
				});
				return null;
			}
		});
	}
	
	private static JavaPairDStream<String, Long> calCulateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream){
		
		JavaPairDStream<String, Long> mappredDStream = filteredAdRealTimeLogDStream.mapToPair(
				new PairFunction<Tuple2<String,String>, String, Long>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						String log = tuple._2;
						String[] logSplited = log.split(" ");
						String timestamp = logSplited[0];
						Date date = new Date(Long.valueOf(timestamp));
						String datekey = DateUtils.formatDateKey(date);
						String provice = logSplited[1];
						String city = logSplited[2];
						long adid = Long.valueOf(logSplited[4]);
						
						String key = datekey +"_"+provice +"_"+city +"_"+adid;
						return new Tuple2<String, Long>(key, 1L);
					}
				});
		JavaPairDStream<String, Long> aggregateDStream = mappredDStream.updateStateByKey(
				new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

					private static final long serialVersionUID = 1L;

					public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
						long clickCount = 0L;
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						
						for(Long value: values) {
							clickCount += value;
						}
						return Optional.of(clickCount);
					}
				});
		aggregateDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						ArrayList<AdStat> adStats = new ArrayList<AdStat>();
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							String[] keySplited = tuple._1.split(" ");
							String date = keySplited[0];
							String province = keySplited[1];
							String city = keySplited[2];
							long adid = Long.valueOf(keySplited[3]);
							
							Long clickCount = tuple._2;
							AdStat adStat = new AdStat();
							adStat.setDate(date);
							adStat.setProvince(province);
							adStat.setCity(city);
							adStat.setAdid(adid);
							adStat.setClickCount(clickCount);
							adStats.add(adStat);
						}
						IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
						adStatDAO.updatebatch(adStats);
					}
				});
				return null;
			}
		});
		return aggregateDStream;
		
	}
	
	private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
		JavaDStream<Row> rowsDstream = adRealTimeStatDStream.transform(
				new Function<JavaPairRDD<String,Long>, JavaRDD<Row>>() {
					private static final long serialVersionUID = 1L;

					public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
						
						JavaPairRDD<String, Long> mappredRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Long>, String, Long>() {

							private static final long serialVersionUID = 1L;

							public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
								String[] keySplit = tuple._1.split(" ");
								String date = keySplit[0];
								String province = keySplit[1];
								long adid = Long.valueOf(keySplit[2]);
								long clickCount = tuple._2;
								
								String key = date+"_"+province+"_"+adid;
								
								return new Tuple2<String, Long>(key, clickCount);
							}
						});
						
						JavaPairRDD<String, Long> dailyAdclickCountByProvinceRDD = mappredRDD.reduceByKey(
								new Function2<Long, Long, Long>() {

									private static final long serialVersionUID = 1L;

									public Long call(Long v1, Long v2) throws Exception {
										return v2=v1;
									}
								});
						JavaRDD<Row> rowsRDD = dailyAdclickCountByProvinceRDD.map(new Function<Tuple2<String,Long>, Row>() {
							private static final long serialVersionUID = 1L;

							public Row call(Tuple2<String, Long> tuple) throws Exception {
								String[] keySplited = tuple._1.split(" ");
								String datekey = keySplited[0];
								String province = keySplited[1];
								long adid = Long.valueOf(keySplited[2]);
								long clickCount = tuple._2;
								String date = DateUtils.formateDate(DateUtils.parseDateKey(datekey));
								return RowFactory.create(date,province,adid,clickCount);
							}
						});
						
						StructType schema = DataTypes.createStructType(Arrays.asList(
								DataTypes.createStructField("date", DataTypes.StringType , true),
								DataTypes.createStructField("province", DataTypes.StringType, true),
								DataTypes.createStructField("ad_id", DataTypes.LongType, true),
								DataTypes.createStructField("click_count", DataTypes.LongType, true)));
						HiveContext sqlContxet = new HiveContext(rdd.context());
						DataFrame dailyAdclickCountByprovinceDF = sqlContxet.createDataFrame(rowsRDD, schema);
						dailyAdclickCountByprovinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
						DataFrame provinceTop3AdDF = sqlContxet.sql("select "
								+ "date "
								+ ",province,"
								+ "ad_id,"
								+ "click_count,"
								+ "from ("
								+ "select "
								+ "date,"
								+ "province,"
								+ "ad_id,"
								+ "click_count,"
								+ "ROW_NUMBER() over(partition by province order by click-count DESC) rank "
								+ "from tmp_daily_ad_click_count_by_prov "
								+ ") t "
								+ " where rank >3");
						
						return provinceTop3AdDF.javaRDD();
					}
				});
		rowsDstream.foreachRDD(new Function<JavaRDD<Row>, Void>() {

			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Row> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

					private static final long serialVersionUID = 1L;

					public void call(Iterator<Row> iterator) throws Exception {
						ArrayList<AdProviceTop3> adProviceTop3s = new ArrayList<AdProviceTop3>();
						while(iterator.hasNext()) {
							Row row = iterator.next();
							String date = row.getString(0);
							String province = row.getString(1);
							long adid = row.getLong(2);
							long clickcount = row.getLong(3);
							
							AdProviceTop3 adProviceTop3 = new AdProviceTop3();
							adProviceTop3.setDate(date);
							adProviceTop3.setProvice(province);
							adProviceTop3.setAdid(adid);
							adProviceTop3.setClickCount(clickcount);
							
							adProviceTop3s.add(adProviceTop3);
						}
						IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
						adProvinceTop3DAO.updateBatch(adProviceTop3s);
					}
				});
				return null;
			}
		});
	}
	
	private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
				String[] logSplited = tuple._2.split(" ");
				String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
				long adid = Long.valueOf(logSplited[4]);
				return new Tuple2<String, Long>(timeMinute+"_"+adid, 1L);
			}
		});
		JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Long call(Long v1, Long v2) throws Exception {
				return v1+v2;
			}
		}, Durations.seconds(60),Durations.seconds(10));
		aggrRDD.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						ArrayList<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							String[] keySplited = tuple._1.split(" ");
							String dateMinute = keySplited[0];
							long adid = Long.valueOf(keySplited[1]);
							Long clickCount = tuple._2;
							
							String date = DateUtils.formateDate(
									DateUtils.parseDateKey(dateMinute.substring(0,8)));
							String hour = dateMinute.substring(8, 10);
							String  minute = dateMinute.substring(10);
							AdClickTrend adClickTrend = new AdClickTrend();
							adClickTrend.setDate(date);
							adClickTrend.setHour(hour);
							adClickTrend.setMinute(minute);
							adClickTrend.setAdid(adid);
							adClickTrend.setClickCount(clickCount);
							
							adClickTrends.add(adClickTrend);
						}
//						IAd
						IaClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
						adClickTrendDAO.updateBatch(adClickTrends);
					}
				});
				return null;
			}
		});
	}
}
