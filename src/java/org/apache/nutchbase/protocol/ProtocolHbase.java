/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutchbase.protocol;

// Hadoop imports

import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutchbase.plugin.PluggableHbase;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutchbase.util.hbase.RowPart;

/** A retriever of url content.  Implemented by protocol extensions. */
public interface ProtocolHbase extends PluggableHbase, Configurable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = ProtocolHbase.class.getName();
  
  /**
   * Property name. If in the current configuration this property is set to
   * true, protocol implementations should handle "politeness" limits
   * internally. If this is set to false, it is assumed that these limits are
   * enforced elsewhere, and protocol implementations should not enforce them
   * internally.
   */
  public final static String CHECK_BLOCKING = "protocol.plugin.check.blocking";

  /**
   * Property name. If in the current configuration this property is set to
   * true, protocol implementations should handle robot exclusion rules
   * internally. If this is set to false, it is assumed that these limits are
   * enforced elsewhere, and protocol implementations should not enforce them
   * internally.
   */
  public final static String CHECK_ROBOTS = "protocol.plugin.check.robots";

  /** Returns the {@link Content} for a fetchlist entry.
   */
  ProtocolOutput getProtocolOutput(String url, RowPart row);

  /**
   * Retrieve robot rules applicable for this url.
   * @param url url to check
   * @param row Row
   * @return robot rules (specific for this url or default), never null
   */
  RobotRules getRobotRules(String url, ImmutableRowPart row);  
}
