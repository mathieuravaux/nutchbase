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

package org.apache.nutch.admin;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

/**
 * Holds instance related properties.
 */
public class NutchInstance {

	private Path fInstanceFolder;
	private Configuration fInstanceConf;
	private String fInstanceName;

	public NutchInstance(String name, Path folder, Configuration instanceConf) {
		this.fInstanceName = name;
		this.fInstanceFolder = folder;
		this.fInstanceConf = instanceConf;
	}

	public String getName() {
		return fInstanceName;
	}

	public Configuration getConfiguration() {
		return this.fInstanceConf;
	}

	public Path getInstanceFolder() {
		return fInstanceFolder;
	}



}
