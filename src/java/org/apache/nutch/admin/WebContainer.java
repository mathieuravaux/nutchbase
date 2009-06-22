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
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

import javax.servlet.Servlet;

//import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRuntimeException;

import org.mortbay.http.SocketListener;

import org.mortbay.jetty.servlet.HashSessionManager;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.Server;
import org.mortbay.http.BasicAuthenticator;
import org.mortbay.http.SecurityConstraint;
import org.mortbay.http.SocketListener;
import org.mortbay.http.UserRealm;
import org.mortbay.http.handler.SecurityHandler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.WebApplicationContext;


//import com.sun.xml.internal.ws.transport.http.DeploymentDescriptorParser;

/**
 * Embed {@link Servlet} container.
 */
public class WebContainer extends Thread {

	private static final Log LOG = LogFactory.getLog(WebContainer.class);

	private Server fServer;
  
	public WebContainer(int port, Configuration configuration) {
		this.fServer = new Server();
		String className = configuration.get("admin.gui.realm", "org.apache.nutch.admin.DefaultRealm");
		
		try {
			Configurable realm = (Configurable) Class.forName(className).newInstance();
			realm.setConf(configuration);
			this.fServer.addRealm( (UserRealm) realm);
		} catch (Exception e) {
			LOG.warn("unable to add realm: " + StringUtils.stringifyException(e));
		}
		SocketListener listener = new SocketListener();
		listener.setPort(port);
		this.fServer.addListener(listener);
	}

	public void run() {
		try {
			this.fServer.start();
		} catch (Exception e) {
			LOG.warn(StringUtils.stringifyException(e));
		}
	}

	/**
	 * starts the web container.
	 * @throws IOException if container couldn't started
	 */
	public void startContainer() throws IOException {
		start();
		try {
			Thread.sleep(3000);
		} catch (InterruptedException ie) {
			LOG.error(StringUtils.stringifyException( ie ));
		}
		if (this.fServer == null || !this.fServer.isStarted()) {
			throw new IOException("Could not start web container");
		}
	}

	/**
	 * stops the web container
	 * @throws Exception 
	 */
	public void stopContainer() throws Exception {
		if (this.fServer != null && this.fServer.isStarted()) {
			this.fServer.stop();
		}
	}

  
	/**
	  * Deploys a set of {@link GuiComponent} extentsions as web applications.
	  * 
	  * @param extensions
	  * @param theInstance
	  * @param allInstances
	  * @throws Exception
	  */
	public void addComponentExtensions(Extension[] extensions,
		NutchInstance theInstance, NutchInstance[] allInstances) throws Exception {
		
		// Instantiate and configure
		ArrayList<GuiComponent> componentList = new ArrayList<GuiComponent>();
		for (int i = 0; i < extensions.length; i++) {
			try {
				Extension extension = extensions[i];
				GuiComponent component = (GuiComponent) extension.getExtensionInstance();
				component.configure(extension, theInstance);
				componentList.add(component);
			} catch (PluginRuntimeException e) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
		
		GuiComponent[] components = (GuiComponent[]) componentList.toArray(
				new GuiComponent[componentList.size()]);

		for (int i = 0; i < components.length; i++) {
			GuiComponent component = components[i];
			Extension extension = component.getExtension();
			String jspFolder = extension.getAttribute(GuiComponent.JSP_FOLDER);
			
			if (jspFolder == null) {
				jspFolder = "jsp";
			}
			
			String jsps = extension.getDescriptor().getPluginPath() + File.separator 
					+ jspFolder + File.separator;
			
			String contextName = "";
			String plugName = extension.getDescriptor().getPluginId();
			
			if (plugName.equals("admin-listing")) {
				contextName = theInstance.getName() + "/";
			} else {
				contextName = theInstance.getName() + "/" + extension.getDescriptor().getPluginId();
			}
			
			
		    WebApplicationContext webContext = 
		          this.fServer.addWebApplication(
		              contextName, 
		              new File(jsps).getCanonicalPath()
		            );
		        
		        
		        webContext.setClassLoader(extension.getDescriptor().getClassLoader());
		        webContext.setAttribute("component", component);
		        webContext.setAttribute("components", components);
		        if (allInstances != null) {
		          webContext.setAttribute("instances", allInstances);
		          webContext.setAttribute("container", this);
		        }

		        SecurityHandler handler = new SecurityHandler();
		        handler.setAuthMethod("BASIC");
		        webContext.addHandler(handler);
		        webContext.setAuthenticator(new BasicAuthenticator());
		        SecurityConstraint sc = new SecurityConstraint();
		        sc.setAuthenticate(true);
		        sc.addRole(SecurityConstraint.ANY_ROLE);
		        webContext.addSecurityConstraint("/", sc);

		        webContext.start();
    }
		
  }
  
}
