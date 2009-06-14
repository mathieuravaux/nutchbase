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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.core.SchedulingContext;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;

/**
 * 
 * This class implements a <code>{@link org.quartz.spi.JobStore}</code> that
 * utilizes the FileSystem as its storage device.
 */
public class FileJobStore implements JobStore {

  private static Log log = LogFactory.getLog(FileJobStore.class);

  // persistent fields
  private HashMap fJobsByName;

  private HashMap fTriggersByName = new HashMap();

  private HashMap fCalendarsByName = new HashMap();

  private HashMap fTriggerStatesByName = new HashMap();

  private HashSet fPausedTriggerGroups = new HashSet();

  // transient fields
  private SchedulerSignaler fSignaler;

  private long fMisfireThreshold = 5001;

  private FileJobStoreSerializer fSerializer;

  private String fStoreDirectory = "quartzstore";

  private HashMap fTriggersByGroup = new HashMap();

  private HashMap fJobsByGroup = new HashMap();

  private TreeSet fOrderedTriggers = new TreeSet();

  private HashSet fBlockedJobs = new HashSet();

  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
      throws SchedulerConfigException {
    this.fSignaler = signaler;
    try {
      this.fSerializer = new FileJobStoreSerializer(this.fStoreDirectory);
      initPeristentFields();
    } catch (JobPersistenceException e) {
      throw new SchedulerConfigException("could not load persistent data ", e);
    }
    fillTransientFields();
    log.info("FileJobStore initialized : "
        + this.fSerializer.getStoreDirectory().getAbsolutePath());
  }

  public void schedulerStarted() throws SchedulerException {
  // nothing to do
  }

  public void shutdown() {
  // nothing to do
  }

  public boolean supportsPersistence() {
    return true;
  }

  public void storeJobAndTrigger(
      SchedulingContext ctxt, JobDetail newJob, Trigger newTrigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    storeJob(ctxt, newJob, false);
    storeTrigger(ctxt, newTrigger, false);

  }

  public void storeJob(
      SchedulingContext ctxt, JobDetail newJobDetail, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    boolean jobExists =
        this.fJobsByName.containsKey(newJobDetail.getFullName());
    if (!replaceExisting && jobExists) {
      throw new ObjectAlreadyExistsException(newJobDetail);
    } else if (replaceExisting && jobExists) {
      log.warn("overwriting existing job: " + newJobDetail.getFullName());
    }

    synchronized (this.fJobsByName) {
      if (!jobExists) {
        addJobGroup(newJobDetail);
      }
      this.fJobsByName.put(newJobDetail.getFullName(), newJobDetail);
      this.fSerializer.saveJobs(this.fJobsByName);
    }
  }

  private void addJobGroup(JobDetail newJob) {
    HashMap groupMap = (HashMap) this.fJobsByGroup.get(newJob.getGroup());
    if (groupMap == null) {
      groupMap = new HashMap();
      this.fJobsByGroup.put(newJob.getGroup(), groupMap);
    }
    groupMap.put(newJob.getName(), newJob);
  }

  /**
   * 
   * @param group
   * @param name
   * @return the full name of a job or a trigger (i.e.
   *         'getFullName(trigger.getGroup(),trigger.getName())' should equal
   *         'trigger.getFullName()')
   */
  private static String getFullName(String group, String name) {
    StringBuffer buffer = new StringBuffer(group);
    buffer.append(".");
    buffer.append(name);
    return buffer.toString();
  }

  public boolean removeJob(
      SchedulingContext ctxt, String jobName, String groupName)
      throws JobPersistenceException {
    JobDetail job =
        (JobDetail) this.fJobsByName.get(getFullName(groupName, jobName));
    if (job == null) {
      return false;
    }

    Trigger[] triggers = getTriggersForJob(ctxt, jobName, groupName);
    for (int i = 0; i < triggers.length; i++) {
      removeTrigger(ctxt, triggers[i].getName(), triggers[i].getGroup());
    }

    synchronized (this.fJobsByName) {
      HashMap groupMap = (HashMap) this.fJobsByGroup.get(groupName);
      if (groupMap != null) {
        groupMap.remove(jobName);
        if (groupMap.size() == 0) {
          this.fJobsByGroup.remove(groupName);
        }
      }
      this.fJobsByName.remove(job.getFullName());
      this.fBlockedJobs.remove(job);
      this.fSerializer.saveJobs(this.fJobsByName);
    }

    return true;
  }

  public JobDetail retrieveJob(
      SchedulingContext ctxt, String jobName, String groupName)
      throws JobPersistenceException {

    return (JobDetail) this.fJobsByName.get(getFullName(groupName, jobName));
  }

  public void storeTrigger(
      SchedulingContext ctxt, Trigger newTrigger, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {

    if (this.fTriggersByName.containsKey(newTrigger.getFullName())) {
      if (!replaceExisting) {
        throw new ObjectAlreadyExistsException(newTrigger);
      } else {
        log.warn("overwriting existing trigger: " + newTrigger.getFullName());
      }
      removeTrigger(ctxt, newTrigger.getName(), newTrigger.getGroup());
    }

    if (!this.fJobsByName.containsKey(newTrigger.getFullJobName())) {
      throw new JobPersistenceException("The job ("
          + newTrigger.getFullJobName()
          + ") referenced by the trigger does not exist.");
    }

    synchronized (this.fTriggersByName) {
      addTriggerGroup(newTrigger);
      this.fTriggersByName.put(newTrigger.getFullName(), newTrigger);
      this.fSerializer.saveTriggers(this.fTriggersByName);
      synchronized (this.fPausedTriggerGroups) {
        if (this.fPausedTriggerGroups.contains(newTrigger.getGroup())) {
          setTriggerState(ctxt, newTrigger, Trigger.STATE_PAUSED);
        } else {
          this.fOrderedTriggers.add(newTrigger);
        }
      }
    }
  }

  private void addTriggerGroup(Trigger newTrigger) {
    HashMap groupMap =
        (HashMap) this.fTriggersByGroup.get(newTrigger.getGroup());
    if (groupMap == null) {
      groupMap = new HashMap();
      this.fTriggersByGroup.put(newTrigger.getGroup(), groupMap);
    }
    groupMap.put(newTrigger.getName(), newTrigger);
  }

  public boolean removeTrigger(
      SchedulingContext ctxt, String triggerName, String groupName)
      throws JobPersistenceException {
    synchronized (this.fTriggersByName) {
      // remove from triggers map
      Trigger trigger =
          (Trigger) this.fTriggersByName.remove(getFullName(
              groupName, triggerName));
      if (trigger == null) {
        return false;
      }
      this.fSerializer.saveTriggers(this.fTriggersByName);

      // remove from triggers by group
      HashMap grpMap = (HashMap) this.fTriggersByGroup.get(groupName);
      if (grpMap != null) {
        grpMap.remove(triggerName);
        if (grpMap.size() == 0)
          this.fTriggersByGroup.remove(groupName);
      }
      this.fOrderedTriggers.remove(trigger);
      JobDetail jobDetail =
          retrieveJob(ctxt, trigger.getJobName(), trigger.getJobGroup());

      // remove state
      setTriggerState(ctxt, trigger, Trigger.STATE_NONE);

      if (!jobDetail.isDurable()
          && getTriggersForJob(ctxt, jobDetail.getName(), jobDetail.getGroup()).length == 0) {
        removeJob(ctxt, jobDetail.getName(), jobDetail.getGroup());
      }
    }

    return true;
  }

  public boolean replaceTrigger(
      SchedulingContext ctxt, String triggerName, String groupName,
      Trigger newTrigger) throws JobPersistenceException {
    Trigger oldTrigger = retrieveTrigger(ctxt, triggerName, groupName);
    if (oldTrigger == null)
      return false;
    if (!oldTrigger.getJobName().equals(newTrigger.getJobName())
        || !oldTrigger.getJobGroup().equals(newTrigger.getJobGroup()))
      throw new JobPersistenceException(
          "New trigger is not related to the same job as the old trigger.");

    JobDetail jobDetail =
        retrieveJob(ctxt, oldTrigger.getJobName(), oldTrigger.getJobGroup());
    synchronized (this.fTriggersByName) {
      removeTrigger(ctxt, triggerName, groupName);
      if (!this.fJobsByName.containsKey(oldTrigger.getFullJobName())) {
        storeJob(ctxt, jobDetail, false);
      }
      storeTrigger(ctxt, newTrigger, false);
    }
    return true;
  }

  public Trigger retrieveTrigger(
      SchedulingContext ctxt, String triggerName, String groupName)
      throws JobPersistenceException {
    return (Trigger) this.fTriggersByName.get(getFullName(
        groupName, triggerName));
  }

  public void storeCalendar(
      SchedulingContext ctxt, String name, Calendar calendar,
      boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException, JobPersistenceException {

    boolean calenderExists = this.fCalendarsByName.containsKey(name);
    if (calenderExists) {
      if (!replaceExisting) {
        throw new ObjectAlreadyExistsException("Calendar with name '"
            + name + "' already exists.");
      }
      this.fCalendarsByName.remove(name);
    }
    this.fCalendarsByName.put(name, calendar);
    this.fSerializer.saveCalendars(this.fCalendarsByName);

    // update triggers
    if (calenderExists && updateTriggers) {
      synchronized (this.fTriggersByName) {
        Trigger[] triggers = getTriggerForCalendar(name);
        for (int i = 0; i < triggers.length; i++) {
          boolean removed = this.fOrderedTriggers.remove(triggers[i]);
          triggers[i].updateWithNewCalendar(calendar, getMisfireThreshold());
          if (removed) {
            this.fOrderedTriggers.add(triggers[i]);
          }
        }
      }
    }
  }

  public boolean removeCalendar(SchedulingContext ctxt, String calName)
      throws JobPersistenceException {
    if (getTriggerForCalendar(calName).length > 0) {
      throw new JobPersistenceException(
          "Calender cannot be removed if it referenced by a Trigger!");
    }

    boolean exists = this.fCalendarsByName.remove(calName) != null;
    if (exists) {
      this.fSerializer.saveCalendars(this.fCalendarsByName);
    }
    return exists;
  }

  public Calendar retrieveCalendar(SchedulingContext ctxt, String calName)
      throws JobPersistenceException {
    return (Calendar) this.fCalendarsByName.get(calName);
  }

  public int getNumberOfJobs(SchedulingContext ctxt)
      throws JobPersistenceException {
    return this.fJobsByName.size();
  }

  public int getNumberOfTriggers(SchedulingContext ctxt)
      throws JobPersistenceException {
    return this.fTriggersByName.size();
  }

  public int getNumberOfCalendars(SchedulingContext ctxt)
      throws JobPersistenceException {
    return this.fCalendarsByName.size();
  }

  public String[] getJobNames(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    HashMap groupMap = (HashMap) this.fJobsByGroup.get(groupName);
    if (groupMap == null)
      return new String[0];

    return (String[]) groupMap.keySet().toArray(
        new String[groupMap.keySet().size()]);
  }

  public String[] getTriggerNames(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    HashMap groupMap = (HashMap) this.fTriggersByGroup.get(groupName);
    if (groupMap == null)
      return new String[0];

    return (String[]) groupMap.keySet().toArray(
        new String[groupMap.keySet().size()]);
  }

  public String[] getJobGroupNames(SchedulingContext ctxt)
      throws JobPersistenceException {
    return (String[]) this.fJobsByGroup.keySet().toArray(
        new String[this.fJobsByGroup.keySet().size()]);
  }

  public String[] getTriggerGroupNames(SchedulingContext ctxt)
      throws JobPersistenceException {
    return (String[]) this.fTriggersByGroup.keySet().toArray(
        new String[this.fTriggersByGroup.keySet().size()]);
  }

  public String[] getCalendarNames(SchedulingContext ctxt)
      throws JobPersistenceException {
    return (String[]) this.fCalendarsByName.keySet().toArray(
        new String[this.fCalendarsByName.keySet().size()]);
  }

  public Trigger[] getTriggersForJob(
      SchedulingContext ctxt, String jobName, String groupName)
      throws JobPersistenceException {
    String jobFullName = getFullName(groupName, jobName);
    ArrayList triggers = new ArrayList();
    synchronized (this.fTriggersByName) {
      for (Iterator iter = this.fTriggersByName.values().iterator(); iter
          .hasNext();) {
        Trigger trigger = (Trigger) iter.next();
        if (trigger.getFullJobName().equals(jobFullName))
          triggers.add(trigger);
      }
    }
    return (Trigger[]) triggers.toArray(new Trigger[triggers.size()]);
  }

  private Trigger[] getTriggerForCalendar(String calName) {
    ArrayList triggers = new ArrayList();
    synchronized (this.fTriggersByName) {
      for (Iterator iter = this.fTriggersByName.values().iterator(); iter
          .hasNext();) {
        Trigger trigger = (Trigger) iter.next();
        if (calName.equals(trigger.getCalendarName())) {
          triggers.add(trigger);
        }
      }
    }

    return (Trigger[]) triggers.toArray(new Trigger[triggers.size()]);
  }

  /*
   * trigger_states and their intern implementation
   * 
   * STATE_NONE - not in TriggersByName .................................
   * STATE_NORMAL - not in TriggerStatesByName ..........................
   * STATE_COMPLETE - with int value in TriggerStatesByName .............
   * STATE_ERROR - with int value in TriggerStatesByName ................
   * STATE_PAUSED - with int value in TriggerStatesByName ...............
   * STATE_BLOCKED - has job in BlockedJobs .............................
   */
  public int getTriggerState(
      SchedulingContext ctxt, String triggerName, String triggerGroup)
      throws JobPersistenceException {
    Trigger trigger = retrieveTrigger(ctxt, triggerName, triggerGroup);
    if (trigger == null)
      return Trigger.STATE_NONE;

    Integer triggerState =
        (Integer) this.fTriggerStatesByName.get(trigger.getFullName());
    boolean jobBlocked = this.fBlockedJobs.contains(trigger.getFullJobName());
    if (jobBlocked) {
      if (triggerState == null
          || triggerState.intValue() != Trigger.STATE_PAUSED)
        return Trigger.STATE_BLOCKED;
    }

    if (triggerState == null)
      return Trigger.STATE_NORMAL;

    return triggerState.intValue();
  }

  private void setTriggerState(
      SchedulingContext ctxt, Trigger trigger, int state)
      throws JobPersistenceException {
    int oldState = getTriggerState(null, trigger.getName(), trigger.getGroup());
    if (oldState == Trigger.STATE_COMPLETE && oldState != Trigger.STATE_NONE)
      return;

    switch (state) {
    case Trigger.STATE_NONE:
      this.fTriggerStatesByName.remove(trigger.getFullName());
      this.fSerializer.saveTriggerStates(this.fTriggerStatesByName);
      break;
    case Trigger.STATE_NORMAL:
      this.fTriggerStatesByName.remove(trigger.getFullName());
      this.fSerializer.saveTriggerStates(this.fTriggerStatesByName);
      break;
    case Trigger.STATE_BLOCKED:
      // do nothing cause the state will be identified by blocked jobs (so
      // we'll not overwrite paused-,error-states)
      break;
    default:
      // otherwise(pause,error,complete) just write the state
      this.fTriggerStatesByName.put(trigger.getFullName(), new Integer(state));
      this.fSerializer.saveTriggerStates(this.fTriggerStatesByName);
      break;
    }
  }

  public void pauseTrigger(
      SchedulingContext ctxt, String triggerName, String groupName)
      throws JobPersistenceException {
    int triggerState = getTriggerState(ctxt, triggerName, groupName);
    if (triggerState != Trigger.STATE_NORMAL
        && triggerState != Trigger.STATE_ERROR)
      return;

    Trigger trigger = retrieveTrigger(ctxt, triggerName, groupName);
    synchronized (this.fTriggersByName) {
      setTriggerState(ctxt, trigger, Trigger.STATE_PAUSED);
      this.fOrderedTriggers.remove(this.fTriggersByName.get(trigger
          .getFullName()));
    }
  }

  public void pauseTriggerGroup(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      if (this.fPausedTriggerGroups.contains(groupName))
        return;

      this.fPausedTriggerGroups.add(groupName);
      this.fSerializer.savePausedTriggerGroups(this.fPausedTriggerGroups);
      String[] names = getTriggerNames(ctxt, groupName);
      for (int i = 0; i < names.length; i++) {
        pauseTrigger(ctxt, names[i], groupName);
      }
    }
  }

  public void pauseJob(SchedulingContext ctxt, String jobName, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      Trigger[] triggers = getTriggersForJob(ctxt, jobName, groupName);
      for (int j = 0; j < triggers.length; j++) {
        pauseTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
      }
    }
  }

  public void pauseJobGroup(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      String[] jobNames = getJobNames(ctxt, groupName);
      for (int i = 0; i < jobNames.length; i++) {
        Trigger[] triggers = getTriggersForJob(ctxt, jobNames[i], groupName);
        for (int j = 0; j < triggers.length; j++) {
          pauseTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
        }
      }
    }
  }

  public void resumeTrigger(
      SchedulingContext ctxt, String triggerName, String groupName)
      throws JobPersistenceException {
    int triggerState = getTriggerState(ctxt, triggerName, groupName);
    if (triggerState != Trigger.STATE_PAUSED)
      return;

    Trigger trigger = retrieveTrigger(ctxt, triggerName, groupName);
    synchronized (this.fTriggersByName) {
      if (this.fBlockedJobs.contains(trigger.getFullJobName())) {
        setTriggerState(ctxt, trigger, Trigger.STATE_BLOCKED);
      } else {
        setTriggerState(ctxt, trigger, Trigger.STATE_NORMAL);
      }

      applyMisfire(ctxt, trigger);
      if (getTriggerState(null, triggerName, groupName) == Trigger.STATE_NORMAL) {
        this.fOrderedTriggers.add(trigger);
      }
    }
  }

  private boolean applyMisfire(SchedulingContext ctxt, Trigger trigger)
      throws JobPersistenceException {
    long misfireTime = System.currentTimeMillis() - getMisfireThreshold();
    Date nextFireTime = trigger.getNextFireTime();
    if (nextFireTime.getTime() > misfireTime) {
      return false; // we are in time
    }

    // notify for misfire
    this.fSignaler.notifyTriggerListenersMisfired(trigger);

    // calculate next fire time
    Calendar calender = retrieveCalendar(null, trigger.getCalendarName());
    trigger.updateAfterMisfire(calender);
    if (trigger.getNextFireTime() == null) {
      setTriggerState(ctxt, trigger, Trigger.STATE_COMPLETE);
      synchronized (this.fTriggersByName) {
        this.fOrderedTriggers.remove(trigger);
      }
    } else if (nextFireTime.equals(trigger.getNextFireTime()))
      return false;

    return true;
  }

  public void resumeTriggerGroup(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      String[] names = getTriggerNames(ctxt, groupName);
      for (int i = 0; i < names.length; i++) {
        resumeTrigger(ctxt, names[i], groupName);
      }
      this.fPausedTriggerGroups.remove(groupName);
      this.fSerializer.saveTriggerStates(this.fTriggerStatesByName);
    }
  }

  public Set getPausedTriggerGroups(SchedulingContext ctxt)
      throws JobPersistenceException {
    HashSet set = new HashSet();
    set.addAll(this.fPausedTriggerGroups);

    return set;
  }

  public void resumeJob(SchedulingContext ctxt, String jobName, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      Trigger[] triggers = getTriggersForJob(ctxt, jobName, groupName);
      for (int j = 0; j < triggers.length; j++) {
        resumeTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
      }
    }
  }

  public void resumeJobGroup(SchedulingContext ctxt, String groupName)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      String[] jobNames = getJobNames(ctxt, groupName);
      for (int i = 0; i < jobNames.length; i++) {
        Trigger[] triggers = getTriggersForJob(ctxt, jobNames[i], groupName);
        for (int j = 0; j < triggers.length; j++) {
          resumeTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
        }
      }
    }
  }

  public void pauseAll(SchedulingContext ctxt) throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      String[] names = getTriggerGroupNames(ctxt);
      for (int i = 0; i < names.length; i++) {
        pauseTriggerGroup(ctxt, names[i]);
      }
    }
  }

  public void resumeAll(SchedulingContext ctxt) throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroups) {
      String[] names = getTriggerGroupNames(ctxt);

      for (int i = 0; i < names.length; i++) {
        resumeTriggerGroup(ctxt, names[i]);
      }
    }
  }

  public Trigger acquireNextTrigger(SchedulingContext ctxt, long noLaterThan)
      throws JobPersistenceException {
    synchronized (this.fTriggersByName) {
      while (!this.fOrderedTriggers.isEmpty()) {
        Trigger trigger = (Trigger) this.fOrderedTriggers.first();
        this.fOrderedTriggers.remove(trigger);

        // have one canditate
        if (trigger.getNextFireTime() != null && !applyMisfire(ctxt, trigger)) {
          if (trigger.getNextFireTime().getTime() > noLaterThan) {
            this.fOrderedTriggers.add(trigger);
            return null;
          }
          trigger.setFireInstanceId(getFiredTriggerRecordId());
          this.fSerializer.saveTriggers(this.fTriggersByName);
          return (Trigger) trigger.clone();
        }

        if (trigger.getNextFireTime() != null) {
          this.fOrderedTriggers.add(trigger);
        }
      }
    }
    return null;
  }

  public void releaseAcquiredTrigger(SchedulingContext ctxt, Trigger trigger)
      throws JobPersistenceException {
    trigger = (Trigger) this.fTriggersByName.get(trigger.getFullName());
    if (trigger != null) {
      this.fOrderedTriggers.add(trigger);
    }
  }

  public TriggerFiredBundle triggerFired(SchedulingContext ctxt, Trigger trigger)
      throws JobPersistenceException {
    synchronized (this.fTriggersByName) {
      Trigger storeTrigger =
          (Trigger) this.fTriggersByName.get(trigger.getFullName());
      int triggerState =
          getTriggerState(ctxt, trigger.getName(), trigger.getGroup());
      if (triggerState != Trigger.STATE_NORMAL)
        return null;

      Calendar calender =
          retrieveCalendar(ctxt, storeTrigger.getCalendarName());
      Date prevFireTime = trigger.getPreviousFireTime();
      // call triggered on our copy, and the scheduler's copy
      trigger.triggered(calender);
      storeTrigger.triggered(calender);

      TriggerFiredBundle fireBundle =
          new TriggerFiredBundle(
              retrieveJob(ctxt, trigger.getJobName(), trigger.getJobGroup()),
              trigger, calender, false, new Date(), trigger
                  .getPreviousFireTime(), prevFireTime, trigger
                  .getNextFireTime());
      JobDetail job = fireBundle.getJobDetail();

      if (job.isStateful()) {
        Trigger[] triggers =
            getTriggersForJob(ctxt, job.getName(), job.getGroup());
        for (int i = 0; i < triggers.length; i++) {
          setTriggerState(ctxt, triggers[i], Trigger.STATE_BLOCKED);
          this.fOrderedTriggers.remove(triggers[i]);
        }
        this.fBlockedJobs.add(job.getFullName());
      } else if (storeTrigger.getNextFireTime() != null) {
        this.fOrderedTriggers.add(storeTrigger);
      }

      return fireBundle;
    }
  }

  public void triggeredJobComplete(
      SchedulingContext ctxt, Trigger trigger, JobDetail jobDetail,
      int triggerInstCode) throws JobPersistenceException {
    synchronized (this.fTriggersByName) {
      this.fBlockedJobs.remove(jobDetail.getFullName());
      JobDetail oldJobDetail =
          retrieveJob(ctxt, jobDetail.getName(), jobDetail.getGroup());
      Trigger oldTrigger =
          (Trigger) this.fTriggersByName.get(trigger.getFullName());

      updateJobDataMap(jobDetail, oldJobDetail);
      updateTriggerState(ctxt, trigger, oldTrigger, triggerInstCode);
    }
  }

  private void updateJobDataMap(JobDetail newJob, JobDetail oldJob)
      throws JobPersistenceException {
    if (oldJob == null || !newJob.isStateful())
      return;

    JobDataMap newData = newJob.getJobDataMap();
    if (newData != null)
      newData.clearDirtyFlag();
    oldJob.setJobDataMap(newData);
    this.fSerializer.saveJobs(this.fJobsByName);
  }

  private void updateTriggerState(
      SchedulingContext ctxt, Trigger newTrigger, Trigger oldTrigger,
      int triggerInstruction) throws JobPersistenceException {
    if (oldTrigger == null)
      return;

    switch (triggerInstruction) {
    case Trigger.INSTRUCTION_DELETE_TRIGGER:
      if (newTrigger.getNextFireTime() != null)
        removeTrigger(ctxt, newTrigger.getName(), newTrigger.getGroup());
      else if (oldTrigger.getNextFireTime() == null)
        removeTrigger(ctxt, newTrigger.getName(), newTrigger.getGroup());
      break;
    case Trigger.INSTRUCTION_SET_TRIGGER_COMPLETE:
      setTriggerState(ctxt, oldTrigger, Trigger.STATE_COMPLETE);
      this.fOrderedTriggers.remove(oldTrigger);
      break;
    case Trigger.INSTRUCTION_SET_TRIGGER_ERROR:
      setTriggerState(ctxt, oldTrigger, Trigger.STATE_ERROR);
      log.info("Trigger " + oldTrigger.getFullName() + " set to ERROR state.");
      break;
    case Trigger.INSTRUCTION_SET_ALL_JOB_TRIGGERS_COMPLETE:
      Trigger[] triggers1 =
          getTriggersForJob(ctxt, oldTrigger.getJobName(), oldTrigger
              .getJobGroup());
      for (int i = 0; i < triggers1.length; i++) {
        setTriggerState(ctxt, triggers1[i], Trigger.STATE_COMPLETE);
      }
      break;
    case Trigger.INSTRUCTION_SET_ALL_JOB_TRIGGERS_ERROR:
      Trigger[] triggers2 =
          getTriggersForJob(ctxt, oldTrigger.getJobName(), oldTrigger
              .getJobGroup());
      for (int i = 0; i < triggers2.length; i++) {
        setTriggerState(ctxt, triggers2[i], Trigger.STATE_ERROR);
      }
      log.info("All triggers of Job "
          + oldTrigger.getFullJobName() + " set to ERROR state.");
      break;
    }
  }

  // -------------------------non-interface---------------------

  /**
   * @return the number of milliseconds in those the job would be executed
   *         delayed before misfired.
   */
  public long getMisfireThreshold() {

    return this.fMisfireThreshold;
  }

  /**
   * @param misfireThreshold
   *          the number of milliseconds in those the job would be executed
   *          delayed before misfired
   */
  public void setMisfireThreshold(long misfireThreshold) {
    this.fMisfireThreshold = misfireThreshold;
  }

  /**
   * @return Returns the storeFilePath.
   */
  public String getStoreFilePath() {
    return this.fStoreDirectory;
  }

  /**
   * @param storeFilePath
   *          The storeFilePath to set.
   */
  public void setStoreFilePath(String storeFilePath) {
    this.fStoreDirectory = storeFilePath;
  }

  /**
   * Removes all persitent data.
   */
  public void clear() {
    this.fJobsByName.clear();
    this.fTriggersByName.clear();
    this.fCalendarsByName.clear();
    this.fTriggerStatesByName.clear();
    this.fPausedTriggerGroups.clear();

    this.fJobsByGroup.clear();
    this.fTriggersByGroup.clear();
    this.fBlockedJobs.clear();
    this.fOrderedTriggers.clear();

    this.fSerializer.clear();
  }

  private static long ftrCtr = System.currentTimeMillis();

  private synchronized String getFiredTriggerRecordId() {
    return String.valueOf(ftrCtr++);
  }

  // -------------------------persistence-related---------------------

  private void initPeristentFields() throws JobPersistenceException {

    this.fJobsByName = (HashMap) this.fSerializer.loadJobs();
    this.fTriggersByName = (HashMap) this.fSerializer.loadTriggers();
    this.fCalendarsByName = (HashMap) this.fSerializer.loadCalendars();
    this.fTriggerStatesByName = (HashMap) this.fSerializer.loadTriggerStates();
    this.fPausedTriggerGroups =
        (HashSet) this.fSerializer.loadPausedTriggerGroups();

    if (this.fJobsByName == null)
      this.fJobsByName = new HashMap();
    if (this.fTriggersByName == null)
      this.fTriggersByName = new HashMap();
    if (this.fCalendarsByName == null)
      this.fCalendarsByName = new HashMap();
    if (this.fTriggerStatesByName == null)
      this.fTriggerStatesByName = new HashMap();
    if (this.fPausedTriggerGroups == null)
      this.fPausedTriggerGroups = new HashSet();
  }

  private void fillTransientFields() {
    for (Iterator iter = this.fJobsByName.values().iterator(); iter.hasNext();) {
      JobDetail job = (JobDetail) iter.next();
      addJobGroup(job);
    }
    for (Iterator iter = this.fTriggersByName.values().iterator(); iter
        .hasNext();) {
      Trigger trigger = (Trigger) iter.next();
      addTriggerGroup(trigger);
    }
    this.fOrderedTriggers.addAll(this.fTriggersByName.values());
  }
}
