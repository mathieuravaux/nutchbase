package org.apache.nutchbase.plugin;

import java.util.HashSet;
import java.util.Set;

import org.apache.nutch.plugin.Pluggable;

public interface PluggableHbase extends Pluggable {

  public static final Set<String> EMPTY_COLUMNS = new HashSet<String>();
  
  public Set<String> getColumnSet();
}
