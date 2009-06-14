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
import org.apache.nutch.crawl.LinkDb;

public class LinkdbThread extends TaskThread {

  private static final Log LOG = 
    LogFactory.getLog(LinkdbThread.class.getName());

  private LinkDb fLinkdb;

  private Path[] fSegments;

  private Path fLinkdbFile;
 
  public LinkdbThread(Configuration configuration, Path linkdb, Path[] segments) {
    super(configuration);
    this.fLinkdb = new LinkDb(configuration);
    this.fLinkdbFile = linkdb;
    this.fSegments = segments;
  }

  public void run() {
    FileSystem fileSystem = null;
    try {
      this.fMessage = "linkdb.running";
      fileSystem = FileSystem.get(this.fConfiguration);
      Path running = new Path(this.fLinkdbFile, "linkdb.running");
      // create lock files in segments
      for (int i = 0; i < this.fSegments.length; i++) {
        Path file = this.fSegments[i];
        fileSystem.createNewFile(new Path(file, "linkdb.running"));
      }
      // create lockfile in linkdb
      fileSystem.createNewFile(running);

      // this.fLinkdb.invert(this.fLinkdbFile, this.fSegments, true, true);
      this.fLinkdb.invert(this.fLinkdbFile, this.fSegments, true, true, false);
      
      for (int i = 0; i < this.fSegments.length; i++) {
        Path file = this.fSegments[i];
        fileSystem.createNewFile(new Path(file, "invert.done"));
      }
    } catch (IOException e) {
      LOG.warn(e.toString());
      this.fMessage = e.toString();
    } finally {
      try {
        if (fileSystem != null) {
          // delete lock files and create done files
          for (int i = 0; i < this.fSegments.length; i++) {
            Path file = this.fSegments[i];
            fileSystem.delete(new Path(file, "linkdb.running"));
          }
          // delete lock file from linkdb
          fileSystem.delete(new Path(this.fLinkdbFile, "linkdb.running"));
        }
      } catch (IOException e) {
        LOG.warn(e.toString());
      }
    }
  }
}
