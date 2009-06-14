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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutch.indexer.Indexer;

import java.util.List;
import java.util.Arrays;

public class IndexThread extends TaskThread {

  private Indexer fIndexer;

  private Path fCrawldb;

  private Path fLinkdb;

  private Path[] fSegments;

  private Path fIndexFolder;

  private static final Log LOG = 
    LogFactory.getLog(IndexThread.class.getName());

  public IndexThread(Configuration configuration, Path indexFolder,
      Path crawldb, Path linkdb, Path[] segments) {
    super(configuration);
    this.fIndexer = new Indexer(configuration);
    this.fIndexFolder = indexFolder;
    this.fCrawldb = crawldb;
    this.fLinkdb = linkdb;
    this.fSegments = segments;

  }

  public void run() {
    FileSystem fileSystem = null;
    try {
      this.fMessage = "index.running";
      fileSystem = FileSystem.get(this.fConfiguration);

      // create lock files in segments
      for (int i = 0; i < this.fSegments.length; i++) {
        Path file = this.fSegments[i];
        fileSystem.createNewFile(new Path(file, "index.running"));
      }

      // create lock files in linkdb
      fileSystem.createNewFile(new Path(this.fLinkdb, "index.running"));
      // create lock files in crawldb
      fileSystem.createNewFile(new Path(this.fCrawldb, "index.running"));
      
      // this.fIndexer.index(
      //     this.fIndexFolder, 
      //     this.fCrawldb, 
      //     this.fLinkdb,
      //     this.fSegments
      //     );
      List<Path> seglist = Arrays.asList(this.fSegments);
      this.fIndexer.index(
          this.fIndexFolder, 
          this.fCrawldb, 
          this.fLinkdb,
          seglist
          );

    } catch (IOException e) {
      LOG.warn(e.toString());
      this.fMessage = e.toString();
    } finally {
      try {
        if (fileSystem != null) {
          // delete lock files from segments
          for (int i = 0; i < this.fSegments.length; i++) {
            Path file = this.fSegments[i];
            fileSystem.delete(new Path(file, "index.running"));
          }
          // delete lock files from linkdb
          fileSystem.delete(new Path(this.fLinkdb, "index.running"));
          // delete lock files from crawldb
          fileSystem.delete(new Path(this.fCrawldb, "index.running"));
        }
      } catch (IOException e) {
        LOG.warn(e.toString());
      }
    }
  }
}
