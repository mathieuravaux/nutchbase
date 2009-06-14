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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Administration Application
 */
public class AdministrationApp extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(AdministrationApp.class.getName());

	private void startJobTracker(final Configuration defaultConf) {
		Runnable jobTrackerStarter = new Runnable() {
			public void run() {
				try {
					String jobtracker = defaultConf.get("mapred.job.tracker", "local");
					if (!"local".equals(jobtracker)) {
						JobConf jobconf = new JobConf(getConf());
						JobTracker.startTracker(jobconf);
						Thread.sleep(3000);
					}
				} catch (IOException e) {
					LOG.error(e.toString());
				} catch (InterruptedException e) {
					LOG.error(e.toString());
				}
			}
		};
		Thread t = new Thread(jobTrackerStarter);
		t.start();
	}

	private AdministrationApp(Configuration conf) {
		setConf(conf);
	}

	/**
	 * starts a container and deploys all gui plugins
	 */
	public WebContainer startContainer(Path initialInstance, Configuration defaultConf) throws Exception {

		int port = defaultConf.getInt("admin.gui.port", 50060);
		WebContainer webContainer = new WebContainer(port, defaultConf);
		webContainer.startContainer();
		System.out.println("Nutch administration interface listening on *:" + port);
		
		NutchInstance[] nutchInstances = getInstances(defaultConf, initialInstance);
		// add all general-components
		Extension[] generalGuiComponents = 
			getComponentExtensions(defaultConf, GuiComponent.IS_GENERAL_COMPONENT);
		NutchInstance generalInstance = 
			new NutchInstance("general", initialInstance, defaultConf);
		webContainer.addComponentExtensions(
				generalGuiComponents, generalInstance, nutchInstances);

		// add instance-components
		for (int i = 0; i < nutchInstances.length; i++) {
			NutchInstance instance = nutchInstances[i];
			Extension[] extensions = getComponentExtensions(
					instance.getConfiguration(), GuiComponent.IS_INSTANCE_COMPONENT);
			webContainer.addComponentExtensions(extensions, instance, null);
		}
		return webContainer;
	}

	/**
	 * @param conf
	 * @param attributeName
	 *          attribute value must be set to "true" in plugin.xml
	 * @return extensions implementing {@link GuiComponent} 
	 *          and matching the attribute filter
	 */
	public static Extension[] getComponentExtensions(Configuration conf,
			String attributeName) {
		ArrayList<Extension> list = new ArrayList<Extension>();
		ExtensionPoint extensionPoint = 
			PluginRepository.get(conf).getExtensionPoint(GuiComponent.X_POINT_ID);
		if (extensionPoint == null) {
			throw new RuntimeException("x-point " 
					+ GuiComponent.X_POINT_ID
					+ " not found, check your plugin folder");
		}
		Extension[] extensions = extensionPoint.getExtensions();
		for (int i = 0; i < extensions.length; i++) {
			Extension extension = extensions[i];
			if (extension.getAttribute(attributeName) != null
					&& extension.getAttribute(attributeName).toLowerCase().equals("true")) {
				list.add(extension);
			}
		}
		return (Extension[]) list.toArray(new Extension[list.size()]);

	}

	/* scans the root folder for instance folders */
	private NutchInstance[] getInstances(Configuration defaultConf, Path instancesRoot) throws IOException{
		//Path[] files = instancesRoot.listFiles();

		// Path[] files =  fs.listPaths(instancesRoot);
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] filestatuses = fs.listStatus(instancesRoot);
		int len = filestatuses.length;
		Path[] files = new Path[len];
		for (int i=0; i < len; i++) {
			files[i] = filestatuses[i].getPath();
		}

		ArrayList<NutchInstance> instancesList = new ArrayList<NutchInstance>();
		for (int i = 0; i < files.length; i++) {
			Path folder = files[i];
			if ( fs.isDirectory(folder) && !folder.getName().equals("conf")) {
				try {
					instancesList.add(loadNutchInstance(defaultConf, folder));
				} catch (IOException e) {
					LOG.warn("unable to load instance: " + e.toString());
				}
			} 
		}
		return (NutchInstance[]) 
		instancesList.toArray(new NutchInstance[instancesList.size()]);
	}

	/**
	 * creates an instance object from a instance folder
	 * 
	 * @param defaultConf
	 * @param folder
	 * @return an instance representation of this folder
	 * @throws IOException
	 *           in case the folder is not a valid instance folder
	 */
	public static NutchInstance loadNutchInstance(Configuration defaultConf, Path folder)
	throws IOException {
		Path instanceConfFolder = new Path(folder, "conf");

		Configuration conf = NutchConfiguration.create(); 
		FileSystem fs = FileSystem.get( conf );
		if (fs.exists(instanceConfFolder) && fs.isDirectory(instanceConfFolder)) {
			Path instanceSiteConf = new Path(instanceConfFolder, "nutch-site.xml");
			if ( fs.exists(instanceSiteConf)) {
				Configuration instanceConf = new Configuration(defaultConf);
				instanceConf.addResource(instanceSiteConf.makeQualified(fs));
				return new NutchInstance(folder.getName(), folder, instanceConf);
			}
		}
		throw new IOException("not a valid instance folder: "
				+ folder);
	}

	private void createFirstInstance(Path file) throws IOException {
		GuiConfigUtil.createConfiguration(file);
		Path defaultInstance = new Path(file, "default");
		GuiConfigUtil.createConfiguration(defaultInstance);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = NutchConfiguration.create();
		int res = ToolRunner.run( conf, new AdministrationApp(conf), args);
		System.exit(res);
	}

	/**
	 * Starts the nutch administration web interface
	 * 
	 * @param args
	 * @throws Exception
	 */
	public int run (String[] args) throws Exception {
		String usage = "Usage: <instancesFolder>";
		if (args.length != 1) {
			System.err.println(usage);
			return 127;
		}
		Configuration defaultConf = NutchConfiguration.create();
		FileSystem fs = FileSystem.get(defaultConf);
		AdministrationApp app = new AdministrationApp(defaultConf);
		Path file = new Path(args[0]);

		if (!fs.exists(file)) {
			app.createFirstInstance(file);
		}
		app.startJobTracker(defaultConf);

		try {
			WebContainer container = app.startContainer(file, defaultConf);
			container.join();
			while(true) { Thread.sleep(250); }

		} catch (Exception e) {
			LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
		}
		LOG.info("Exiting normally...");
		return 0;
	}



}
