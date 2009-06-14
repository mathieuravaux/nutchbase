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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.quartz.JobPersistenceException;

/**
 * FileJobStoreSerializer is responsible for writing the relevant data of
 * FileJobStore to files.
 * 
 * <p/> Note:only the write/save methods are synchronized cause it is assumed
 * that load and clean operations invoked synchronous.
 * 
 */
public class FileJobStoreSerializer {

  private File fJobsFile;

  private File fTriggersFile;

  private File fCalendarsFile;

  private File fTriggerStatesFile;

  private File fPausedTriggerGroupsFile;

  /**
   * @param storeDirectory
   * @throws JobPersistenceException
   */
  public FileJobStoreSerializer(String storeDirectory)
      throws JobPersistenceException {
    
    File storeDirectoryFile = new File(storeDirectory);
    storeDirectoryFile.mkdirs();
    if (!storeDirectoryFile.exists()) {
      throw new JobPersistenceException(
          "could not create the store directory "
          + storeDirectoryFile.getPath()
          );
    }

    this.fJobsFile = new File(storeDirectoryFile, "jobs.dat");
    this.fTriggersFile = new File(storeDirectoryFile, "triggers.dat");
    this.fCalendarsFile = new File(storeDirectoryFile, "calendars.dat");
    this.fTriggerStatesFile =
        new File(storeDirectoryFile, "trigger-states.dat");
    this.fPausedTriggerGroupsFile =
        new File(storeDirectoryFile, "paused-trigger-groups.dat");
  }

  /**
   * @return the root directory of the store
   */
  public File getStoreDirectory() {
    return this.fJobsFile.getParentFile();
  }

  /**
   * Cleans the store to conceive a empty store.
   */
  public void clear() {
    this.fJobsFile.delete();
    this.fTriggersFile.delete();
    this.fCalendarsFile.delete();
    this.fTriggerStatesFile.delete();
    this.fPausedTriggerGroupsFile.delete();
  }

  /**
   * @param jobs
   * @throws JobPersistenceException
   */
  public void saveJobs(Serializable jobs) throws JobPersistenceException {
    synchronized (this.fJobsFile) {
      saveObjectToFile(this.fJobsFile, jobs);
    }

  }

  /**
   * @param triggers
   * @throws JobPersistenceException
   */
  public void saveTriggers(Serializable triggers)
      throws JobPersistenceException {
    synchronized (this.fTriggersFile) {
      saveObjectToFile(this.fTriggersFile, triggers);
    }
  }

  /**
   * @param calendars
   * @throws JobPersistenceException
   */
  public void saveCalendars(Serializable calendars)
      throws JobPersistenceException {
    synchronized (this.fCalendarsFile) {
      saveObjectToFile(this.fCalendarsFile, calendars);
    }
  }

  /**
   * @param triggerStates
   * @throws JobPersistenceException
   */
  public void saveTriggerStates(Serializable triggerStates)
      throws JobPersistenceException {
    synchronized (this.fTriggerStatesFile) {
      saveObjectToFile(this.fTriggerStatesFile, triggerStates);
    }
  }

  /**
   * @param pausedTriggerGroups
   * @throws JobPersistenceException
   */
  public void savePausedTriggerGroups(Serializable pausedTriggerGroups)
      throws JobPersistenceException {
    synchronized (this.fPausedTriggerGroupsFile) {
      saveObjectToFile(this.fPausedTriggerGroupsFile, pausedTriggerGroups);
    }
  }

  /**
   * @return jobs
   * @throws JobPersistenceException
   */
  public Serializable loadJobs() throws JobPersistenceException {
    return loadObjectsFromFile(this.fJobsFile);
  }

  /**
   * @return triggers
   * @throws JobPersistenceException
   */
  public Serializable loadTriggers() throws JobPersistenceException {
    return loadObjectsFromFile(this.fTriggersFile);
  }

  /**
   * @return calendars
   * @throws JobPersistenceException
   */
  public Serializable loadCalendars() throws JobPersistenceException {
    return loadObjectsFromFile(this.fCalendarsFile);
  }

  /**
   * @return trigger-states
   * @throws JobPersistenceException
   */
  public Serializable loadTriggerStates() throws JobPersistenceException {
    return loadObjectsFromFile(this.fTriggerStatesFile);
  }

  /**
   * @return paused trigger groups
   * @throws JobPersistenceException
   */
  public Serializable loadPausedTriggerGroups() throws JobPersistenceException {
    return loadObjectsFromFile(this.fPausedTriggerGroupsFile);
  }

  private void saveObjectToFile(File file, Serializable serializable)
      throws JobPersistenceException {
    try {
      FileOutputStream fileOutStream = new FileOutputStream(file);
      ObjectOutputStream objOutStream = new ObjectOutputStream(fileOutStream);
      objOutStream.writeObject(serializable);
      fileOutStream.close();
    } catch (IOException e) {
      throw new JobPersistenceException("could not store to file "
          + file.getPath(), e);
    }
  }

  private Serializable loadObjectsFromFile(File file)
      throws JobPersistenceException {
    if (!file.exists() || file.length() == 0)
      return null;

    try {
      FileInputStream fileInStream = new FileInputStream(file);
      ObjectInputStream objInStream = new ObjectInputStream(fileInStream);
      Serializable serializable = (Serializable) objInStream.readObject();
      objInStream.close();
      fileInStream.close();
      return serializable;
    } catch (Exception e) {
      throw new JobPersistenceException("could not load from file "
          + file.getPath(), e);
    }
  }
}
