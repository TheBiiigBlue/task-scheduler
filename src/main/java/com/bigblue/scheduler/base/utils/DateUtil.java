package com.bigblue.scheduler.base.utils;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName(类名) :     DateUtil
 * @Description(描述) :   日期工具类
 * @author(作者) ：        yubo
 * @date (开发日期)      ：2018/4/2
 */
public class DateUtil {
    // 默认日期格式
    public static final String DATE_DEFAULT_FORMAT = "yyyy-MM-dd";
    // 默认日期格式
    public static final String DATE_DEFAULT_FORMAT_ONE = "yyyyMMdd";

    // 默认时间格式
    public static final String DATETIME_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    // 默认月份格式1
    public static final String DATETIME_DEFALUT_MONTH_FORMAT = "yyyy-MM";
    // 默认月份格式2
    public static final String DATETIME_DEFALUT_MONTH_FORMAT_TWO = "yyyy.MM";

    public static final String TIME_DEFAULT_FORMAT = "HH:mm:ss";

    // 日期格式化
    private static DateFormat dateFormat = null;

    // 时间格式化
    private static DateFormat dateTimeFormat = null;

    private static DateFormat timeFormat = null;

    private static Calendar gregorianCalendar = null;

    private static DateFormat dateDefaultFormat = null;
    private static String DATE_BUSINESS_NAME = "日期工具类模块";


    static {
        dateFormat = new SimpleDateFormat(DATE_DEFAULT_FORMAT);
        dateDefaultFormat = new SimpleDateFormat(DATE_DEFAULT_FORMAT_ONE);
        dateTimeFormat = new SimpleDateFormat(DATETIME_DEFAULT_FORMAT);
        timeFormat = new SimpleDateFormat(TIME_DEFAULT_FORMAT);
        gregorianCalendar = new GregorianCalendar();
    }

    /**
     * @return java.util.Date
     * @Author wugz
     * @Description 获取当前时间 date格式
     * @Date 2019/2/19 10:50
     * @Param []
     **/
    public static Date nowDate() {
        try {
            return dateTimeFormat.parse(getDateTimeFormat(new Date()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 日期格式化yyyy-MM-dd
     *
     * @param date
     * @return
     */
    public static Date formatDate(String date, String format) {
        try {
            return new SimpleDateFormat(format).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 日期格式化yyyy-MM-dd
     *
     * @param date
     * @return
     */
    public static String getDateFormat(Date date) {
        return dateFormat.format(date);
    }

    /**
     * 日期格式化yyyy-MM-dd HH:mm:ss
     *
     * @param date
     * @return
     */
    public static String getDateTimeFormat(Date date) {
        return dateTimeFormat.format(date);
    }

    /**
     * 时间格式化
     *
     * @param date
     * @return HH:mm:ss
     */
    public static String getTimeFormat(Date date) {
        return timeFormat.format(date);
    }

    /**
     * 日期格式化
     *
     * @param date
     * @return
     */
    public static Date getDateFormat(String date) {
        try {
            return dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 时间格式化
     *
     * @param date
     * @return
     */
    public static Date getDateTime(String date) {
        try {
            return dateTimeFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取当前日期(yyyy-MM-dd)
     *
     * @return
     */
    public static Date getNowDate() {
        return DateUtil.getDateFormat(dateFormat.format(new Date()));
    }


    /**
     * 日期格式化yyyyMMdd
     *
     * @param date
     * @return
     */
    public static String getNowDateFormat(Date date) {
        return dateDefaultFormat.format(date);
    }

    public static Date parseDateFormat(String dateStr) {
        Date date = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            date = sdf.parse(dateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 获取当前日期星期一日期
     *
     * @return date
     */
    public static Date getFirstDayOfWeek() {
        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
        gregorianCalendar.setTime(new Date());
        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek()); // Monday
        return gregorianCalendar.getTime();
    }

    /**
     * 获取当前日期星期日日期
     *
     * @return date
     */
    public static Date getLastDayOfWeek() {
        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
        gregorianCalendar.setTime(new Date());
        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek() + 6); // Monday
        return gregorianCalendar.getTime();
    }

    /**
     * 获取日期星期一日期
     *
     * @param date 指定日期
     * @return date
     */
    public static Date getFirstDayOfWeek(Date date) {
        if (date == null) {
            return null;
        }
        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
        gregorianCalendar.setTime(date);
        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek()); // Monday
        return gregorianCalendar.getTime();
    }

    /**
     * 获取日期星期一日期
     *
     * @param date 指定日期
     * @return date
     */
    public static Date getLastDayOfWeek(Date date) {
        if (date == null) {
            return null;
        }
        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
        gregorianCalendar.setTime(date);
        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek() + 6); // Monday
        return gregorianCalendar.getTime();
    }

    /**
     * 获取当前月的第一天
     *
     * @return date
     */
    public static Date getFirstDayOfMonth() {
        gregorianCalendar.setTime(new Date());
        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取当前月的最后一天
     *
     * @return
     */
    public static Date getLastDayOfMonth() {
        gregorianCalendar.setTime(new Date());
        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
        gregorianCalendar.add(Calendar.MONTH, 1);
        gregorianCalendar.add(Calendar.DAY_OF_MONTH, -1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取指定月的第一天
     *
     * @param date
     * @return
     */
    public static Date getFirstDayOfMonth(Date date) {
        gregorianCalendar.setTime(date);
        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取指定月的最后一天
     *
     * @param date
     * @return
     */
    public static Date getLastDayOfMonth(Date date) {
        gregorianCalendar.setTime(date);
        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
        gregorianCalendar.add(Calendar.MONTH, 1);
        gregorianCalendar.add(Calendar.DAY_OF_MONTH, -1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取日期前一天
     *
     * @param date
     * @return
     */
    public static Date getDayBefore(Date date) {
        gregorianCalendar.setTime(date);
        int day = gregorianCalendar.get(Calendar.DATE);
        gregorianCalendar.set(Calendar.DATE, day - 1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取日期后一天
     *
     * @param date
     * @return
     */
    public static Date getDayAfter(Date date) {
        gregorianCalendar.setTime(date);
        int day = gregorianCalendar.get(Calendar.DATE);
        gregorianCalendar.set(Calendar.DATE, day + 1);
        return gregorianCalendar.getTime();
    }

    /**
     * 获取当前年
     *
     * @return
     */
    public static int getNowYear() {
        Calendar d = Calendar.getInstance();
        return d.get(Calendar.YEAR);
    }

    /**
     * 获取当前月份
     *
     * @return
     */
    public static int getNowMonth() {
        Calendar d = Calendar.getInstance();
        return d.get(Calendar.MONTH) + 1;
    }

    /**
     * 获取当月天数
     *
     * @return
     */
    public static int getNowMonthDay() {
        Calendar d = Calendar.getInstance();
        return d.getActualMaximum(Calendar.DATE);
    }

    /**
     * 获取时间段的每一天
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 日期列表
     */
    public static List<Date> getEveryDay(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return null;
        }
        // 格式化日期(yy-MM-dd)
        startDate = DateUtil.getDateFormat(DateUtil.getDateFormat(startDate));
        endDate = DateUtil.getDateFormat(DateUtil.getDateFormat(endDate));
        List<Date> dates = new ArrayList<Date>();
        gregorianCalendar.setTime(startDate);
        dates.add(gregorianCalendar.getTime());
        while (gregorianCalendar.getTime().compareTo(endDate) < 0) {
            // 加1天
            gregorianCalendar.add(Calendar.DAY_OF_MONTH, 1);
            dates.add(gregorianCalendar.getTime());
        }
        return dates;
    }

    /****
     * 传入具体日期 ，返回具体日期增加一个月。
     * @param date 日期(2017-04)
     * @return 2017-05
     * @throws ParseException
     */
    public static String getNextMonth(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(date);
        rightNow.add(Calendar.MONTH, 1);
        Date dt1 = rightNow.getTime();
        String reStr = sdf.format(dt1);
        return reStr;
    }

    /**
     * 获取提前多少个月
     *
     * @param monty
     * @return
     */
    public static Date getFirstMonth(int monty) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, -monty);
        return c.getTime();
    }

    /**
     * @param now ：当前需转换的日期
     * @param num ：距离之前的天数
     * @return ：字符串类型的格式化的前几天
     * @Description(描述) ：格式化前几天
     */
    public static String getBeforeData(Date now, int num) {
        Date bDate = new Date();
        //得到日历
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        calendar.add(Calendar.DAY_OF_MONTH, num);  //设置为前一天
        bDate = calendar.getTime();   //得到前一天的时间
        String defaultStartDate = dateFormat.format(bDate);    //格式化前一天
        return defaultStartDate;
    }

    /**
     * 获取两个月份之差
     *
     * @param startMonth
     * @param endMonth
     * @return
     */
    public static Integer getMonthDValue(String startMonth, String endMonth) {
        boolean format = true;
        SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_DEFALUT_MONTH_FORMAT);
        SimpleDateFormat sdf2 = new SimpleDateFormat(DATETIME_DEFALUT_MONTH_FORMAT_TWO);
        Calendar bef = Calendar.getInstance();
        Calendar aft = Calendar.getInstance();
        try {
            bef.setTime(sdf.parse(startMonth));
            aft.setTime(sdf.parse(endMonth));
        } catch (ParseException e) {
            e.printStackTrace();
            format = false;
        }
        if (!format) {
            try {
                bef.setTime(sdf2.parse(startMonth));
                aft.setTime(sdf.parse(endMonth));
            } catch (ParseException e) {
                e.printStackTrace();
                return -1;
            }
        }
        int result = aft.get(Calendar.MONTH) - bef.get(Calendar.MONTH);
        int month = (aft.get(Calendar.YEAR) - bef.get(Calendar.YEAR)) * 12;

        return Math.abs(month + result);
    }

    /**
     * <li>功能描述：时间相减得到天数
     *
     * @param beginDateStr
     * @param endDateStr
     * @return Integer
     * @author Administrator
     */
    public static Integer getDaySub(String beginDateStr, String endDateStr) {
        Integer day = 0;
        Date beginDate = formatDate(beginDateStr, DATETIME_DEFAULT_FORMAT);
        Date endDate = formatDate(endDateStr, DATETIME_DEFAULT_FORMAT);
        day = (int) ((endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000));
        return day;
    }

    public static String getCurrentFormatDateLong19() {
        return getCurrentFormatDate("yyyy-MM-dd HH:mm:ss");
    }


    public static String getCurrentFormatDate(String formatDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
        Date dateInstance = getCurrentDate();
        simpleDateFormat.applyPattern(formatDate);
        return simpleDateFormat.format(dateInstance);
    }

    public static Date getCurrentDate() {
        return new Date();
    }

    /**
     * 把字符串的yyyyMMdd转换为yyyy-MM-dd格式
     *
     * @param str 传入需要格式时间
     * @return
     */
    public static String dateParse(String str) {
        String frt = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_DEFAULT_FORMAT_ONE);
            SimpleDateFormat sdf2 = new SimpleDateFormat(DATE_DEFAULT_FORMAT);
            frt = sdf2.format(sdf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return frt;
    }
}
