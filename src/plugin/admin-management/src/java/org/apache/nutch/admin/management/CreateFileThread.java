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


public class CreateFileThread extends TaskThread {

  private static final Logger LOG = 
    Logger.getLogger(CreateFileThread.class.getName());

  private Path fFile;

  public CreateFileThread(Configuration configuration, Path file) {
    super(configuration);
    this.fFile = file;
  }

  public void run() {
    try {
      this.fMessage = "file.create.running";
      FileSystem system = FileSystem.get(this.fConfiguration);
      system.createNewFile(this.fFile);
    } catch (IOException e) {
      LOG.warning(e.toString());
      this.fMessage = e.toString();
    }
  }

}
