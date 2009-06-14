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

import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginClassLoader;

/**
 * A default implementation of the {@link GuiComponent} extension point. 
 */
public class DefaultGuiComponent implements GuiComponent {

	private static final Logger LOG = Logger.getLogger(DefaultGuiComponent.class.getName());
	private HashMap fResourceBundles = new HashMap();
	private Extension fExtension;
	private NutchInstance fNutchInstance;

	public void configure(Extension extension, NutchInstance instance) {
		this.fExtension = extension;
		this.fNutchInstance = instance;
	}

	public String getLabel(String key, Locale locale) {
		String value = key;
		ResourceBundle labels = (ResourceBundle) this.fResourceBundles.get(locale);
		if (labels == null) {
			try {
				PluginClassLoader classLoader = this.fExtension.getDescriptor().getClassLoader();
				String bundleName = this.fExtension.getAttribute("bundle");
				labels = ResourceBundle.getBundle(bundleName, locale, classLoader);
				this.fResourceBundles.put(locale, labels);
			} catch (Exception e) {
				LOG.warning("unable to load resource bundle: "+e.toString());
			}
		}
		value = labels.getString(key);
		return value;
	}

	public NutchInstance getNutchInstance() {
		return this.fNutchInstance;
	}

	public Extension getExtension() {
		return this.fExtension;
	}

}
