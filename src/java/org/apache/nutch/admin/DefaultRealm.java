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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.http.HashUserRealm;;

/**
 * A simple Realm implementation that adds just one user "admin" 
 * and looks up a password from the set configuration.
 * 
 * Nutch realms need to implement {@link Configurable} and 
 * {@link UserRealm}.
 *
 */
public class DefaultRealm extends HashUserRealm implements Configurable {

	private Configuration fConf;

	public void setConf(Configuration conf) {
		this.fConf = conf;
		String password = conf.get("admin.gui.realm.password", "admin");
		put("admin", password);

	}

	public Configuration getConf() {
		return this.fConf;
	}

}
