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

import java.util.Locale;

import org.apache.nutch.plugin.Extension;

/**
 * An Extension point definition for functional administration interface
 * components.
 * 
 * The nutch functional administration interface is built form a set of plug-ins
 * that are plugged together into a embedded web container. The extension point,
 * the plug-ins have to implement, is named GuiComponent.
 * 
 * To be able to manage multiple different configured deployments (e.g. intranet
 * and home-page) we introduce the concept of so called instances. Instances use
 * the same nutch code base and one general configuration, but can have
 * properties that overwrite the general configured properties. Also each
 * instance has a own data folder (local or dfs) where the databases, segments
 * and indexes are stored.
 * 
 * Since there is a general nutch configuration and several nutch instances we
 * have two kind of plug-ins: general plug-ins and instance plug-ins. Instance
 * plug-ins only have access to the instance- folder and configuration but
 * general plug-ins have access to all instances and the general configuration.
 * 
 * To define a plug-in as general- or instance-depending, the attributes
 * "isGeneralComponent" or "isInstanceComponent" can be defined as true. It is
 * also possible, that a plugin can be deployed in both contexts.
 * 
 * 
 * The most important part of an administration interface plugin are jsp pages.
 * Each plugin need at least one index.jsp page that is located in a folder that
 * can be defined in the plugin.xml as attribute "jspFolder" of the extension
 * node. Also a attribute "tabName" is required, where the value is used to show
 * the component index page in a navigation bar.
 * 
 * To make administration interface plugin development easier, we provide a
 * <code>DefaultGuiComponent</code> that handles all functionalities and
 * provides internationalization support. So only the
 * <code>DefaultGuiComponent</code> definition in the plugin.xml, required attributes, a
 * set of jsp pages and i18n bundles need to be defined to get a component
 * implemented.
 */
public interface GuiComponent {

  /** extension point id */
  public final static String X_POINT_ID = GuiComponent.class.getName();

  /**
   * Attribute determinate if a component is general.
   */
  public static final String IS_GENERAL_COMPONENT = "isGeneralComponent";

  /**
   * Attribute determinate if a component is instance specific.
   */
  public static final String IS_INSTANCE_COMPONENT = "isInstanceComponent";

  /**
   * Attribute defines the folder inside the plugin folder contains jsp and jsp
   * snippets.
   */
  public static final String JSP_FOLDER = "jspFolder";

  /**
   * Configures a component. Method is only triggered once until gui startup
   * 
   * @param extension
   *          providing access to plugin.xml attributes
   * @param instance
   *          providing access to configuration properties
   */
  public void configure(Extension extension, NutchInstance instance);

  /**
   * @param key
   * @param locale
   * @return localized values
   */
  public String getLabel(String key, Locale locale);

  /**
   * @return via configure injected extension
   */
  public Extension getExtension();

  /**
   * @return via configure injected instance
   */
  public NutchInstance getNutchInstance();
}
