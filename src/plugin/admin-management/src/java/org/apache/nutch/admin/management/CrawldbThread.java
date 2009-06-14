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

package org.apache.nutch.admin.management;

import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutch.crawl.CrawlDb;

/**
 * Runs a crawlDB task
 * 
 */
public class CrawldbThread extends TaskThread {

  private CrawlDb fCrawldb;

  private Path fSegment;

  private Path fCrawldbFile;

  private static final Logger LOG = 
    Logger.getLogger(CrawldbThread.class.getName());

  
  public CrawldbThread(Configuration configuration, Path crawldb, Path segment) {
    super(configuration);
    this.fCrawldb = new CrawlDb(configuration);
    this.fCrawldbFile = crawldb;
    this.fSegment = segment;
  }

  public void run() {

    Path runningSegment = null;
    Path runningDB = null;
    FileSystem fileSystem = null;
    try {
      this.fMessage = "crawldb.running";
      fileSystem = FileSystem.get(this.fConfiguration);
      runningSegment = new Path(this.fSegment, "crawldb.running");
      runningDB = new Path(this.fCrawldbFile, "crawldb.running");
      fileSystem.createNewFile(runningSegment);
      fileSystem.createNewFile(runningDB);
      // this.fCrawldb.update(this.fCrawldbFile, this.fSegment, true, true);
      Path[] segs = new Path[1];
      segs[0] = this.fSegment;
      this.fCrawldb.update(this.fCrawldbFile, segs, true, true);

    } catch (IOException e) {
      LOG.warning(e.toString());
      this.fMessage = e.toString();
    } finally {
      try {
        if (fileSystem != null) {
          fileSystem.delete(runningSegment);
          fileSystem.delete(runningDB);
        }
      } catch (IOException e) {
        LOG.warning(e.toString());
      }
    }
  }
}
