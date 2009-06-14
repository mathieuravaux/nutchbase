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

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.admin.TaskThread;

public class DeleteFileThread extends TaskThread {

  private static final Logger LOG = 
    Logger.getLogger(DeleteFileThread.class.getName());

  private Path[] fFiles;

  private Configuration fConfiguration;

  /**
   * @param configuration
   * @param files 
   */
  public DeleteFileThread(Configuration configuration, Path[] files) {
    super(configuration);
    this.fConfiguration = configuration;
    this.fFiles = files;
  }

  public void run() {
    try {
      this.fMessage = "file.delete.running";
      for (int i = 0; i < this.fFiles.length; i++) {
        Path file = this.fFiles[i];
        FileSystem.get(this.fConfiguration).delete(file);  
      }
    } catch (IOException e) {
      LOG.warning(e.toString());
      this.fMessage = e.toString();
    }
  }
}
