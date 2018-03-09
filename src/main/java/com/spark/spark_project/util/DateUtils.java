package com.spark.spark_project.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

		public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
		public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
		
		public static boolean before(String time1,String time2)  {
			try {
				Date dateparse1 = TIME_FORMAT.parse(time1);
				Date dateparse2 = TIME_FORMAT.parse(time2);
				
				if(dateparse1.before(dateparse2)) {
					return true;
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			
			return false;
		}


		public static boolean after(String time1,String time2) {
			try {
				Date dateparse1 = TIME_FORMAT.parse(time1);
				Date dateparse2 = TIME_FORMAT.parse(time2);
				
				if(dateparse1.after(dateparse2)) {
					return true;
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			return false;
		}
		
		/**
		 * 计算时间差值
		 * */
		public static int minus(String time1,String time2) {
			try {
				Date dateparse1 = TIME_FORMAT.parse(time1);
				Date dateparse2 = TIME_FORMAT.parse(time2);
				
				long minllisecond = dateparse1.getTime() - dateparse2.getTime();
				return Integer.valueOf(String.valueOf(minllisecond/1000));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return 0;
		}
		
		public static String getDateHour(String datetime) {
			
			String date = datetime.split(" ")[3];
			String hourMiniteSecond = date.split(" ")[1];
			String hour = hourMiniteSecond.split(":")[1];
			return date+"_"+hour;
		}
		
		public static String getTodayDate() {
			return DATE_FORMAT.format(new Date());
		}
		
		public static String getYesterday() {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DAY_OF_YEAR, 1);
			Date date = cal.getTime();
			return DATE_FORMAT.format(date); 
		}
		
		public static String formateDate(Date date) {
			return DATE_FORMAT.format(date);
		}
		
		public static String formatTime(String date) {
			return TIME_FORMAT.format(date);
		}
		
		public static Date parseTime(String time) {
			try {
				return TIME_FORMAT.parse(time);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		public static String formatDateKey(Date date) {
			return DATEKEY_FORMAT.format(date);
		}
		
		public static Date parseDateKey(String datekey) {
			try {
				return DATEKEY_FORMAT.parse(datekey);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		public static String formatTimeMinute(Date date) {
			SimpleDateFormat sdf  = new SimpleDateFormat("yyyyMMddHHmm");
			return sdf.format(date);
		}
	}

