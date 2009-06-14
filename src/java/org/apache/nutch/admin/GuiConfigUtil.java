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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Bundles some methods to handle configuration files
 */
public class GuiConfigUtil {

	/**
	 * Creates a new configuration, then writes the new configuration to the 
	 * path
	 * @param folder the path of the new configuration
	 * @return fresh loaded configuration
	 */
	public static Configuration loadNewConfiguration(Path folder) throws IOException{
		Configuration configuration = NutchConfiguration.create();
		configure(configuration, folder);
		return configuration;
	}

	/**
	 * Creates a configuration folder and stores nutch's configuration files 
	 * in this folder.
	 * 
	 * @param instanceFolder
	 * @throws IOException
	 */
	public static void createConfiguration(Path instanceFolder) throws IOException {
		Path confFolder = new Path(instanceFolder, "conf");
		FileSystem fs = FileSystem.get( NutchConfiguration.create() );
		fs.mkdirs(confFolder);
		copyConfigurationFiles(confFolder);
	}

	private static void copyConfigurationFiles(Path target) throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get( NutchConfiguration.create() );

		InputStream in = AdministrationApp.class.getResourceAsStream("/nutch-default.xml");
		OutputStream out = new FileOutputStream( new File( new File(target.makeQualified(fs).toUri()), "nutch-default.xml") );
		copyContent(in, out);

		in = AdministrationApp.class.getResourceAsStream("/nutch-site.xml");
		out = new FileOutputStream( new File( new File(target.makeQualified(fs).toUri()), "nutch-site.xml") );
		copyContent(in, out);
	}

	private static void copyContent(InputStream in, OutputStream out) throws IOException {
		byte[] buf = new byte[1024];
		int len;
		while ((len = in.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		out.flush();
		in.close();
		out.close();
	}

	/**
	 * Push nutch-(default|site).xml from a given  folder/conf 
	 * into a configuration. 
	 * 
	 * @param configuration
	 * @param folder
	 */
	private static void configure(Configuration configuration, Path folder) throws IOException{
		FileSystem fs = FileSystem.get( NutchConfiguration.create() );
		Path confFolder = new Path(folder, "conf");

		if (fs.exists(confFolder)) {
			Path defaultConf = new Path(confFolder, "nutch-default.xml");
			if (fs.exists(defaultConf)) {
				configuration.addResource(defaultConf.makeQualified(fs));
			}

			Path siteConf = new Path(confFolder, "nutch-site.xml");
			if (fs.exists(siteConf)) {
				configuration.addResource(siteConf.makeQualified(fs));
			}
		}
	}
}
