package com.spark.spark_project.spark.product;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.spark.spark_project.conf.ConfigurationManage;
import com.spark.spark_project.constants.Canstants;
import com.spark.spark_project.dao.IAreaTop3ProductDAO;
import com.spark.spark_project.dao.ITaskDAO;
import com.spark.spark_project.dao.factory.DAOFactory;
import com.spark.spark_project.domain.AreaTop3Product;
import com.spark.spark_project.domain.Task;
import com.spark.spark_project.util.ParamUtils;
import com.spark.spark_project.util.SparkUtils;

import scala.Tuple2;
import scala.collection.mutable.HashMap;

public class AreaTop3ProductSpark {
	public static void main(String[] args) throws SQLException {
		SparkConf conf = new SparkConf().setAppName("AreaTop3ProuctSpark");
		SparkUtils.setMaster(conf);
		
		//构建Sparkde 上下文
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		//注册自定义函数
		sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
		sqlContext.udf().register("get_json_object",new GetJsonObjectUDF(), DataTypes.StringType);
		sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
		sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct", new GroupConcatDistantctUDAF());
		
		SparkUtils.mockData(sc, sqlContext);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Canstants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startTime = ParamUtils.getParam(taskParam, Canstants.PARAM_START_TIME);
		String endTime = ParamUtils.getParam(taskParam, Canstants.PARAM_END_DATE);
		
		//查询用户指定日期范围内的点击行为数据（city_id,在哪个城市发生的点击行为）
		//hive的使用
		JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startTime, endTime);
		System.out.println("cityid2clickActionRDD: "+cityid2clickActionRDD.count());
		
		//从Mysql中查询城市信息
		JavaPairRDD<Long,Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
		System.out.println("cityid2clickActionRDD: "+cityid2cityInfoRDD);

		//成点击商品基础信息临时表
		generteTempClickProductBasicTable(sqlContext, cityid2clickActionRDD,cityid2cityInfoRDD);
		
		//生成各区域各商品点击次数的临时表
		generateTempAreaProductClickCountTable(sqlContext);
		
		//生成包含完整商品信息的各区域各商品点击次数的临时表
		generateTempAreaFullProductClickCountTable(sqlContext);
		
		//使用开窗函数获取各个区域内点击次数排名前3的热门商品
		JavaRDD<Row> areaTop3productRDD = getAreaTop3productRDD(sqlContext);
		System.out.println("areaTop3ProductRDD: "+areaTop3productRDD.count());
		
		List<Row> rows = areaTop3productRDD.collect();
		System.out.println("rows: "+rows.size());
		persisitAreatop3Product(taskid, rows);
		
		sc.close();
	}
	
	/**
	 * 查询指定日期范围内的点击行为数据
	 * @param sqlContext
	 * @param startTime
	 * @param endTime
	 * @return 点击行为数据
	 * */
	private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(SQLContext sqlContext ,String startDate,String enDate){
		String sql ="select  "
				+ "city_id ,click_product_id product_id "
				+ "from user_visit_action "
				+ "where date>='"+startDate+"' "
						+ "and date<='"+enDate+"' "
								+ "click_product_is is not null";
		DataFrame clickActionDF = sqlContext.sql(sql);
		JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
		JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				long cityid = row.getLong(0);
				return new Tuple2<Long, Row>(cityid, row);
			}
		});
		
		return cityid2clickActionRDD;
	}
	
	/**
	 * 使用spark sql 从Mysql中查询城市信息
	 * @params sqlContext SQlContext
	 * @return
	 * */
	private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext){
		String url="";
		String user="";
		String password="";
		
		boolean local = ConfigurationManage.getBoolean(Canstants.SPARK_LOCAL);
		if(local) {
			url = ConfigurationManage.getProperties(Canstants.JDBC_URL);
			user = ConfigurationManage.getProperties(Canstants.JDBC_USER);
			password = ConfigurationManage.getProperties(Canstants.JDBC_PASSWORD);
		}else {
			url = ConfigurationManage.getProperties(Canstants.JDBC_URL_PROD);
			user = ConfigurationManage.getProperties(Canstants.JDBC_USER_PROD);
			password = ConfigurationManage.getProperties(Canstants.JDBC_PASSWORD_PROD);
		}
		
		HashMap<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("user", user);
		options.put("password", password);
		
		DataFrame cityInfoDate = sqlContext.read().format("jdbc").options(options).load();
		
		JavaRDD<Row> cityInfoRdd = cityInfoDate.javaRDD();
		JavaPairRDD<Long, Row> cityid2cityinfo = cityInfoRdd.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				long cityid = Long.valueOf(String.valueOf(row.get(0)));
				return new Tuple2<Long, Row>(cityid, row);
			}
		});
		return cityid2cityinfo;
	}
	
	/**
	 * 生成点击商品基础信息临时表
	 * @param sqlContext
	 * @param cityid2clickActionRDD
	 * @param cityid2cityinfoRDD
	 * */
	
	private static void generteTempClickProductBasicTable(
			SQLContext sqlContext ,
			JavaPairRDD<Long, Row> cityid2clickActionRDD,
			JavaPairRDD<Long, Row> cityid2cityinfoRDD) {
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRDD.join(cityid2cityinfoRDD);
		JavaRDD<Row> mappredRDD = joinedRDD.map(new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

			private static final long serialVersionUID = 1L;

			public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
				Long cityid = tuple._1;
				Row clickAction = tuple._2._1;
				Row cityInfo = tuple._2._2;
				long productid = clickAction.getLong(1);
				String cityname = cityInfo.getString(1);
				String area = cityInfo.getString(2);
				
				return RowFactory.create(cityid,cityname,area,productid);
			}
		});
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id",DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
		
		StructType schema = DataTypes.createStructType(structFields);
		DataFrame df = sqlContext.createDataFrame(mappredRDD, schema);
		System.out.println("tmp_click_product_basic: "+df.count());
		
		df.registerTempTable("tmp_click_product_basic");
	}
	
	/**
	 * 生成各区域各商品点击的次数的临时表
	 * @param sqlContext
	 * */
	
	private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {
		//按照area和product_id两个字段进行分组
		//计算出各区域各商品的点击次数
		//可以获取到每个area下的每个product_id的城市的信息拼接起来的字符串
		String sql = "select "
				+ " area,"
				+ " product_id,"
				+ " count(*) click_count,"
				+ " group_concat_distinct(concat_long_string(city_id.city_name,':')) city_infos "
				+ "from tmp_click_proudct_basic "
				+ "group by area,product_id ";
		/**
		 * 双重group by
		 * */
		DataFrame df = sqlContext.sql(sql);
		System.out.println("tmp_area_product_click_count :"+df.count());
		
		df.registerTempTable("tmp_area_product_click_count");
	}
	
	/**
	 * 生成区域商品点击次数临时表（包含了商品的完整信息）
	 * */
	private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
		String sql ="select tapcc.area,tapcc.product_id,tapcc.click_count,tapcccity_infos,"
				+ "pi.product_name,"
				+ "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status "
				+ "from tmp_area_product_click_count tapcc"
				+ " join product_info pi on tapcc.product=pi.product";
		DataFrame df = sqlContext.sql(sql);
		System.out.println("tmp_area_fullprod_click_count :"+df.count());
		df.registerTempTable("tmp_area-fullprod_click_count");
	}
	
	/**
	 * 获取各区域top3热门商品
	 * @param qslContext
	 * @return
	 * */
	private static JavaRDD<Row> getAreaTop3productRDD(SQLContext sqlContext){
		String sql="select area,case "
				+ "when area='Chain North' or area='Chain East' then 'A Level' "
				+ "when area='Chain South' or area='Chain Niddle' then 'B Leval' "
				+ "when area='Chain North' or area='West South' then 'C Level' "
				+ "else 'D Leve' "
				+ "END area_level,"
				+ "product_id,"
				+ "click_count,"
				+ "city_infos,"
				+ "product_name,"
				+ "product_status "
			+ "FROM ("
				+ "SELECT "
					+ "area,"
					+ "product_id,"
					+ "click_count,"
					+ "city_infos,"
					+ "product_name,"
					+ "product_status,"
					+ "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
				+ "FROM tmp_area_fullprod_click_count "
			+ ") t "
			+ "WHERE rank<=3";
		DataFrame df = sqlContext.sql(sql);
		return df.javaRDD();
	}
	
	/**
	 * 将计算出来的各区域top热门商品写入Mysql
	 * @param rows
	 * @throws SQLException 
	 * */
	
	private static void persisitAreatop3Product(long taskid,List<Row> rows) throws SQLException {
		ArrayList<AreaTop3Product> areatop3Products = new ArrayList<AreaTop3Product>();
		for(Row row: rows) {
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductid(row.getLong(2));
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(0))));
			areaTop3Product.setCityInfos(row.getString(4));
			areaTop3Product.setProductName(row.getString(5));
			areaTop3Product.setProductStatus(row.getString(6));
			areatop3Products.add(areaTop3Product);
		}
		IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areaTop3ProductDAO.insertBatch(areatop3Products);
	}
}
