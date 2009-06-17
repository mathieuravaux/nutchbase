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

package org.apache.nutch.admin.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutchbase.crawl.InjectorHbase;
import org.apache.nutchbase.indexer.IndexerHbase;

/**
 * Runs a injections task
 */
public class IndexThread extends TaskThread {

  private String tableName;
  private Path indexDir;
  private IndexerHbase indexer;
  private Configuration conf;
  
  private static final Logger LOG = Logger.getLogger(IndexThread.class.getName());

  public IndexThread(Configuration configuration) {
    super(configuration);
    this.conf = configuration;
    this.indexer = new IndexerHbase();
    indexer.setConf(configuration);
    this.tableName = "webtable";
    this.indexDir = new Path(configuration.get("searcher.dir"), "index");
  }

  public void run() {
    try {
//      this.fMessage = "index.running";
      FileSystem fs = FileSystem.get(this.conf);
      if (fs.exists(this.indexDir)) {
    	  Path backup_path = new Path(this.conf.get("searcher.dir"), "index_backup_" + new Date().getTime());
    	  System.out.println("Backing up " + this.indexDir + " as " + backup_path + "...");
    	  fs.rename(this.indexDir, backup_path);
    	  System.out.println("Done.");
      }
      
      System.out.println("Re-indexing in " + this.indexDir.toString());
      this.indexer.index(this.indexDir, this.tableName);
    } catch (IOException e) {
    	LOG.warning(e.toString());
        this.fMessage = e.toString();
    }
  }

}
