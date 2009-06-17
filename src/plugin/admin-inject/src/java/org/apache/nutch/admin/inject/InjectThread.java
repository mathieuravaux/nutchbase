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

package org.apache.nutch.admin.inject;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutch.crawl.Injector;
import org.apache.nutchbase.crawl.InjectorHbase;

/**
 * Runs a injections task
 */
public class InjectThread extends TaskThread {

  private String tableName;

  private Path fUrlFolder;

  private InjectorHbase fInjector;

  private static final Logger LOG = Logger.getLogger(InjectThread.class.getName());

  public InjectThread(Configuration configuration, Path urlFolder) {
    super(configuration);
    this.fInjector = new InjectorHbase();
    this.fInjector.setConf(configuration);
	this.tableName = "webtable";
    this.fUrlFolder = urlFolder;
  }

  public void run() {
    try {
      this.fMessage = "inject.running";
      this.fInjector.inject(this.tableName, this.fUrlFolder);
    } catch (IOException e) {
      LOG.warning(e.toString());
      this.fMessage = e.toString();
    }
  }

}
