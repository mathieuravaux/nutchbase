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
import org.apache.nutch.parse.ParseSegment;

public class ParseThread extends TaskThread {

  private static final Log LOG = 
    LogFactory.getLog(ParseThread.class);

  private ParseSegment fParser;

  private Path fSegment;
  
  public ParseThread(Configuration configuration, Path segment) {
    super(configuration);
    this.fParser = new ParseSegment(configuration);
    this.fSegment = segment;
  }

  public void run() {
    FileSystem fileSystem = null;
    Path running = null;
    try {
      this.fMessage = "parse.running";
      fileSystem = FileSystem.get(this.fConfiguration);
      running = new Path(this.fSegment, "parse.running");
      fileSystem.createNewFile(running);
      this.fParser.parse(this.fSegment);
      fileSystem.createNewFile(new Path(this.fSegment, "parse.done"));
    } catch (IOException e) {
      LOG.warn(e.toString());
      this.fMessage = e.toString();
    } finally {
      try {
        if (fileSystem != null) {
          fileSystem.delete(running);
        }
      } catch (IOException e) {
        LOG.warn(e.toString());
      }
    }
  }
}
