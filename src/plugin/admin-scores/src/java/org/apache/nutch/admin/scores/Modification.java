package org.apache.nutch.admin.scores;

import java.io.Serializable;


public class Modification implements Serializable{
	public static final String META_PAGERANK = "_Trylog_Pagerank_";
	public static final String META_VOTES = "_Trylog_Votes_";

	String meta;
	float newValue;
	
	public Modification() {}
	public Modification(String _m, float _n) { meta = _m; newValue = _n; }
}
