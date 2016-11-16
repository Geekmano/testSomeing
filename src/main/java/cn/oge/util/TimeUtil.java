package cn.oge.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

	public static String getNow() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(new Date());
	}

	public static Long transStringToLong(String str) {
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return format.parse(str).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Long transStringToLong(String str, String formatParrn) {
		try {
			SimpleDateFormat format = new SimpleDateFormat(formatParrn);
			return format.parse(str).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String transLongToString(Long dateLong) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(dateLong);
		return format.format(date);
	}

	public static Date transStringToDate(String str) {
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return format.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Date transUitlDateStringToDate(String str) {
		try {
			SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", java.util.Locale.ENGLISH);
			return format.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Long transStringToLong(String str, SimpleDateFormat format) {
		try {
			return format.parse(str).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String transLongToString(Long dateLong, SimpleDateFormat format) {
		Date date = new Date(dateLong);
		return format.format(date);
	}

	public static Date transStringToDate(String str, SimpleDateFormat format) {
		try {
			return format.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
//		System.out.println(transStringToLong("2016-07-00 00:00:00"));
		System.out.println(transStringToLong("2015-02-17 13:55:00"));
		System.out.println(transLongToString(1457539200000L));
	}
}
