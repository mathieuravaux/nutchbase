/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.admin.scheduling;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Scheduling service which uses the quartz scheduling framework. For example
 * and explanation of <i>cronExpression</i>s which are used to initialize cron
 * jobs see below(taken from quartz documentation). <p/>
 * 
 * A cron expression has 6 mandatory and 1 optional elements separate with white
 * space. <br/> "sec min hour dayOfMonth Month dayOfWeek Year(optional)" <p/>
 * 
 * An example of a complete cron-expression is the string "0 0 12 ? * WED" which
 * means "every Wednesday at 12:00 pm". <br>
 * <br>
 * 
 * Individual sub-expressions can contain ranges and/or lists. For example, the
 * day of week field in the previous (which reads "WED") example could be
 * replaces with "MON-FRI", "MON, WED, FRI", or even "MON-WED,SAT". <br>
 * <br>
 * 
 * Wild-cards (the '*' character) can be used to say "every" possible value of
 * this field. Therefore the '*' character in the "Month" field of the previous
 * example simply means "every month". A '*' in the Day-Of-Week field would
 * obviously mean "every day of the week". <br>
 * <br>
 * 
 * All of the fields have a set of valid values that can be specified. These
 * values should be fairly obvious - such as the numbers 0 to 59 for seconds and
 * minutes, and the values 0 to 23 for hours. Day-of-Month can be any value
 * 0-31, but you need to be careful about how many days are in a given month!
 * Months can be specified as values between 0 and 11, or by using the strings
 * JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV and DEC. Days-of-Week
 * can be specified as values between 1 and 7 (1 = Sunday) or by using the
 * strings SUN, MON, TUE, WED, THU, FRI and SAT. <br>
 * <br>
 * 
 * The '/' character can be used to specify increments to values. For example,
 * if you put '0/15' in the Minutes field, it means 'every 15 minutes, starting
 * at minute zero'. If you used '3/20' in the Minutes field, it would mean
 * 'every 20 minutes during the hour, starting at minute three' - or in other
 * words it is the same as specifying '3,23,43' in the Minutes field. <br>
 * <br>
 * 
 * The '?' character is allowed for the day-of-month and day-of-week fields. It
 * is used to specify "no specific value". This is useful when you need to
 * specify something in one of the two fields, but not the other. See the
 * examples below for clarification. <br>
 * <br>
 * 
 * The 'L' character is allowed for the day-of-month and day-of-week fields.
 * This character is short-hand for "last", but it has different meaning in each
 * of the two fields. For example, the value "L" in the day-of-month field means
 * "the last day of the month" - day 31 for January, day 28 for February on
 * non-leap years. If used in the day-of-week field by itself, it simply means
 * "7" or "SAT". But if used in the day-of-week field after another value, it
 * means "the last xxx day of the month" - for example "6L" or "FRIL" both mean
 * "the last Friday of the month". When using the 'L' option, it is important
 * not to specify lists, or ranges of values, as you'll get confusing results.
 * <br>
 * <br>
 * 
 * "0 0/25 * * * ?" Fire every 25 minutes<br>
 * 
 * "10 0/5 * * * ?" Fires every 5 minutes, at 10 seconds after the minute (i.e.
 * 10:00:10 am, 10:05:10 am, etc.).</br>
 * 
 * "0 0 12 * * ?" Fire at 12pm (noon) every day<br>
 * 
 * "0 15 10 ? * *" Fire at 10:15am every day<br>
 * 
 * "0 15 10 * * ?" Fire at 10:15am every day<br>
 * 
 * "0 15 10 * * ? *" Fire at 10:15am every day<br>
 * 
 * "0 15 10 * * ? 2005" Fire at 10:15am every day during the year 2005<br>
 * 
 * "0 * 14 * * ?" Fire every minute starting at 2pm and ending at 2:59pm, every
 * day<br>
 * 
 * "0 0/5 14 * * ?" Fire every 5 minutes starting at 2pm and ending at 2:55pm,
 * every day<br>
 * 
 * "0 0/5 14,18 * * ?" Fire every 5 minutes starting at 2pm and ending at
 * 2:55pm, AND fire every 5 minutes starting at 6pm and ending at 6:55pm, every
 * day<br>
 * 
 * "0 0-5 14 * * ?" Fire every minute starting at 2pm and ending at 2:05pm,
 * every day<br>
 * 
 * "0 10,44 14 ? 3 WED" Fire at 2:10pm and at 2:44pm every Wednesday in the
 * month of March.<br>
 * 
 * "0 15 10 ? * MON-FRI"Fire at 10:15am every Monday, Tuesday, Wednesday,
 * Thursday and Friday<br>
 * 
 * "0 15 10 15 * ?" Fire at 10:15am on the 15th day of every month<br>
 * 
 * "0 15 10 L * ?" Fire at 10:15am on the last day of every month<br>
 * 
 * "0 15 10 ? * 6L" Fire at 10:15am on the last Friday of every month<br>
 * 
 * "0 15 10 ? * 6L" Fire at 10:15am on the last Friday of every month<br>
 * 
 * "0 15 10 ? * 6L 2002-2005" Fire at 10:15am on every last Friday of every
 * month during the years 2002, 2003, 2004 and 2005<br>
 * 
 * "0 15 10 ? * 6#3" Fire at 10:15am on the third Friday of every month<br>
 * 
 * <br/><br/>created on 11.10.2005
 * 
 */
public class SchedulingService {

  /**
   * 
   */
  public static final String CRAWL_JOB = "crawl";

  private Scheduler fScheduler;
  private static final Log LOG = LogFactory.getLog(SchedulingService.class);
  
  /**
   * @throws SchedulerException
   * @throws IOException
   */
  public SchedulingService(PathSerializable fileStorePath)
      throws SchedulerException, IOException {
    
    InputStream resourceAsStream =
        SchedulingService.class.getResourceAsStream("/quartz.properties");
    Properties properties = new Properties();
    if (resourceAsStream != null) {
      // load values from config file
      properties.load(resourceAsStream);
    } else {
      // use some default values
      properties.put(
          StdSchedulerFactory.PROP_JOB_STORE_CLASS, 
          FileJobStore.class.getName()
          );
      properties.put(
          StdSchedulerFactory.PROP_THREAD_POOL_CLASS,
          "org.quartz.simpl.SimpleThreadPool"
          );
      properties.put("org.quartz.threadPool.threadCount", "5");

    }
    properties.setProperty(
        "org.quartz.jobStore.storeFilePath", 
        fileStorePath.toString()
        );
    
    SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
    this.fScheduler = schedulerFactory.getScheduler();
    this.fScheduler.start();
  }

  /**
   * @throws SchedulerException
   */
  public void shutdown() throws SchedulerException {
    this.fScheduler.shutdown();
  }

  /**
   * There will be one trigger for job, with the same name and group as the job.
   * All optional parameters could be left blank (null). For
   * cron-expression-examples see class description.
   * 
   * @param jobName
   * @param jobGroup
   *          (optional)
   * @param jobClass
   * @param jobData
   *          (optional), will be available in
   *          Job#execute(org.quartz.JobExecutionContext)
   * @param cronExpression
   * @throws ParseException
   * @throws SchedulerException
   */
  public void scheduleCronJob(
      String jobName, String jobGroup, Class jobClass, Map<String,PathSerializable> jobData,
      String cronExpression) throws ParseException, SchedulerException {
    
    JobDetail jobDetail = new JobDetail(jobName, jobGroup, jobClass);
    if (jobData != null) {
      jobDetail.setJobDataMap(new JobDataMap(jobData));
    }

    CronTrigger trigger = new CronTrigger(jobName, jobGroup);
    trigger.setCronExpression(cronExpression);
    this.fScheduler.deleteJob(jobName, jobGroup);
    this.fScheduler.scheduleJob(jobDetail, trigger);
    
    LOG.info("Job scheduled: jobName: " + jobName + " cronExp: " + cronExpression );
    
  }

  /**
   * Schedule job with striped cronExpression. The cron expression will be
   * integrated from the single parameters sec - year. If you leave those
   * parameter blank, the "*"-character will be inserted. For
   * cron-expression-examples see class description.
   * 
   * There will be one trigger for job, with the same name and group as the job.
   * 
   * All optional parameters could be left blank (null).
   * 
   * @param jobName
   * @param jobGroup
   *          (optional)
   * @param jobClass
   * @param jobData
   *          (optional), will be available in
   *          Job#execute(org.quartz.JobExecutionContext)
   * @param sec
   * @param min
   * @param hours
   * @param dayOfMonth
   * @param month
   * @param dayOfWeek
   * @param year
   * @throws SchedulerException
   * @throws ParseException
   */
  public void scheduleCronJob(
      String jobName, String jobGroup, Class jobClass, Map jobData, String sec,
      String min, String hours, String dayOfMonth, String month,
      String dayOfWeek, String year) throws ParseException, SchedulerException {
    String cronExpression =
        getCronExpression(sec, min, hours, dayOfMonth, month, dayOfWeek, year);
    scheduleCronJob(jobName, jobGroup, jobClass, jobData, cronExpression);
    
    LOG.info("Job scheduled: jobName: " + jobName + " time: " + year + " " + month + " " + dayOfMonth + " " 
    		+ hours + " " + min + " " + sec);
  }

  /**
   * @param jobName
   * @param groupName
   * @return true if job exists
   * @throws SchedulerException
   */
  public boolean removeJob(String jobName, String groupName)
      throws SchedulerException {
    return this.fScheduler.deleteJob(jobName, groupName);
  }

  /**
   * The cron expression will be integrated from the single parameters sec -
   * year. If you leave those parameter blank, the "*"-character will be
   * inserted. For cron-expression-examples see class description.
   * 
   * @param sec
   * @param min
   * @param hours
   * @param dayOfMonth
   * @param month
   * @param dayOfWeek
   * @param year
   * @return the integrated cron expression
   */
  public static String getCronExpression(
      String sec, String min, String hours, String dayOfMonth, String month,
      String dayOfWeek, String year) {
    StringBuffer buffer = new StringBuffer();
    appendNextExpression(buffer, sec);
    appendNextExpression(buffer, min);
    appendNextExpression(buffer, hours);
    appendNextExpression(buffer, dayOfMonth);
    appendNextExpression(buffer, month);
    appendNextExpression(buffer, dayOfWeek);
    appendNextExpression(buffer, year);

    return buffer.toString();
  }

  private static void appendNextExpression(
      StringBuffer buffer, String expression) {
    if (buffer.length() > 0) {
      buffer.append(" ");
    }
    if (expression == null) {
      buffer.append("*");
    } else {
      buffer.append(expression);
    }
  }

  /**
   * @return
   * @throws SchedulerException
   */
  public String getCronExpressions(String jobName, String jobGroup)
      throws SchedulerException {
    String ret = null;
    Trigger trigger = this.fScheduler.getTrigger(jobName, jobGroup);
    if (trigger instanceof CronTrigger) {
      CronTrigger cronTrigger = (CronTrigger) trigger;
      ret = cronTrigger.getCronExpression();
    }
    return ret;
  }

  /**
   * @param jobName
   * @param jobGroup
   * @throws SchedulerException
   */
  public void deleteJob(String jobName, String jobGroup)
      throws SchedulerException {
    this.fScheduler.deleteJob(jobName, jobGroup);
  }

}
