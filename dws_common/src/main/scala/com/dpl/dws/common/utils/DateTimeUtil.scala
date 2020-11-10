package com.dpl.dws.common.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

object DateTimeUtil {
  private val year_month_format:String = "yyyyMM"
  private val date_format:String = "yyyy-MM-dd"
  private val dateTime_format:String = "yyyy-MM-dd HH:mm:ss.SSS"

  val yyyyMMdddateTimeFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val date =new Date()

    println("-------------------------------------------------")
//    val yestoday=getYesterday();
//    println("yestoday:"+yestoday)
//    val yes_begin = getDayBeginTime(yestoday)
//    val yes_end = getDayEndTime(yestoday)
//    println("-------------------------------------------------")
//    println("yes_begin:"+yes_begin)
//    testTime(yes_begin)
    println("-------------------------------------------------")
    //testTime(yes_end)
    getTime("20200923")


  }
  def getTime(inc_date:String): Unit ={
    val year = inc_date.substring(0,4)
    val month = inc_date.substring(4,6).toInt
    val day = inc_date.substring(6).toInt
    println(year,month,day)
    val begin_date_time_utc = getDayBeginTimeStamp(inc_date)
    val end_date_time_utc = getDayEndTimeStamp(inc_date)
    println("begin_date_time_UTC:"+begin_date_time_utc)
    println("end_date_time_UTC:"+end_date_time_utc)
  }

  def parsedateTime(datetime:String): Date ={
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(dateTime_format)
    dateFormat.parse(datetime)
  }


  def getDayBeginTime(day:String): Date ={
    val day_begn = day+" 00:00:00.000"
    parsedateTime(day_begn)
  }

  def getDayEndTime(day:String): Date ={
    val day_end = day+" 23:59:59.999"
    parsedateTime(day_end)
  }

  def testTime(date: Date): Unit ={

    val time1 =getLOcalTimestamp(date)
    val time2 =getISSHTimestamp(date)
    val time3 =getKolkataTimestamp(date)
    val time4 =getIUTCTimestamp(date)
    println("time1:"+time1)
    println("time2:"+time2)
    println("time3:"+time3)
    println("time4:"+time4)
  }

  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(date_format)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


    def getKolkataTimestamp(date: Date): String = {
    val tz = TimeZone.getTimeZone("Asia/Kolkata")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'")
    df.setTimeZone(tz)
    val nowAsISO = df.format(date)
    System.out.println(nowAsISO)
    nowAsISO
  }

  def getLOcalTimestamp(date: Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'")
    val nowAsISO = df.format(date)
    System.out.println(nowAsISO)
    nowAsISO
  }

  def getISSHTimestamp(date: Date): String = {
    val tz = TimeZone.getTimeZone("Asia/Shanghai")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'")
    df.setTimeZone(tz)
    val nowAsISO = df.format(date)
    System.out.println(nowAsISO)
    nowAsISO
  }
  def getIUTCTimestamp(date: Date): String = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'")
    df.setTimeZone(tz)
    val nowAsISO = df.format(date)
    nowAsISO
  }

  def tsToString(t: Timestamp,format:String): String = {
    if(t==null) return null
    val df = new SimpleDateFormat(format)
    df.format(t)
  }

  def timstampToDateTimeFormat(t: Timestamp): String = {
    if(t==null) return null
    tsToString(t,dateTime_format)
  }

  def timstampToYearMonth(t: Timestamp): String = {
    if(t==null) return null
    tsToString(t,year_month_format)
  }


  def getDayBeginTimeStamp(day:String):String={
    val begin_time=day+" 00:00:00.000"
    val begin_date_time=  yyyyMMdddateTimeFormat.parse(begin_time)
    println("begin_date_time:"+begin_date_time)
    getIUTCTimestamp(begin_date_time)
  }

  def getDayEndTimeStamp(day:String):String={
    val end_time=day+" 23:59:59.999"
    val end_date_time=  yyyyMMdddateTimeFormat.parse(end_time)
    println("end_date_time:"+end_date_time)
    getIUTCTimestamp(end_date_time)
  }


}
