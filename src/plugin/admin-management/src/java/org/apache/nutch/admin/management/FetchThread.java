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
import org.apache.nutch.fetcher.Fetcher;

public class FetchThread extends TaskThread {

  private Fetcher fFetcher;

  private Path fSegment;

  private int fThreads;

  private boolean fParsing;

  private static final Logger LOG = 
    Logger.getLogger(FetchThread.class.getName());
  
  public FetchThread(Configuration configuration, Path segment) {
    super(configuration);
    this.fFetcher = new Fetcher(configuration);
    this.fSegment = segment;
    this.fThreads = configuration.getInt("fetcher.threads.fetch", 10);
  }

  public void run() {
    FileSystem fileSystem = null;
    Path running = null;
    try {
      this.fMessage = "fetch.running";
      fileSystem = FileSystem.get(this.fConfiguration);
      running = new Path(this.fSegment, "fetch.running");
      fileSystem.createNewFile(running);
      this.fFetcher.fetch(this.fSegment, this.fThreads, true);
      fileSystem.createNewFile(new Path(this.fSegment, "fetch.done"));
      if (this.fParsing) {
        fileSystem.createNewFile(new Path(this.fSegment, "parse.done"));
      }
    } catch (IOException e) {
      LOG.warning(e.toString());
      this.fMessage = e.toString();
    } finally {
      try {
        if (fileSystem != null) {
          fileSystem.delete(running);
        }
      } catch (IOException e) {
        LOG.warning(e.toString());
      }
    }
  }
}
