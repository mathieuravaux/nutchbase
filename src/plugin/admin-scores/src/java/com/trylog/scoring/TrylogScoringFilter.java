package com.trylog.scoring;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.nutch.admin.scores.Modification;
import org.apache.nutch.admin.scores.ScoreUpdater;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

import org.apache.nutch.admin.scores.Modification;

public class TrylogScoringFilter implements ScoringFilter {
	private static final Logger LOG = Logger.getLogger(TrylogScoringFilter.class.getName());

	private Configuration conf;
	private float scoreInjected = 0.001f;
	private float normalizedScore = 1.00f;
	private float pagerankToVotesRatio = 0.01f;
	
	public TrylogScoringFilter() { }
	
	public Configuration getConf() { return conf; }
	
	public void setConf(Configuration conf) {
		this.conf = conf;
		normalizedScore = conf.getFloat("trylog.scoring.normalize.score", 1.00f);
		scoreInjected = conf.getFloat("trylog.scoring.injected.score", 1.00f);
		pagerankToVotesRatio = conf.getFloat("trylog.scoring.injected.score", 0.01f);
	}
	
	public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
		  CrawlDatum adjust, int allCount) throws ScoringFilterException {
		return adjust;
	}
	
	public float generatorSortValue(Text url, CrawlDatum datum, float initSort) throws ScoringFilterException {
		return datum.getScore() * initSort;
	}
	
	public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum, CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
    		throws ScoringFilterException {
		//LOG.debug("TrylogScoringFilter :: indexerScore");
		float newScore = getScoreFromMetas(url, dbDatum);
	    LOG.debug("Trylog scoring filter new score : " + url + " -> " + newScore);
	    return (normalizedScore * newScore);
	}
	
	public void initialScore(Text url, CrawlDatum datum) throws ScoringFilterException {
		//LOG.debug("TrylogScoringFilter :: initialScore");
		datum.setScore(0.0f);
	}
	
	public void injectedScore(Text url, CrawlDatum datum) throws ScoringFilterException {
		//LOG.debug("TrylogScoringFilter :: injectedScore");
		datum.setScore(scoreInjected);
	}
	
	public void passScoreAfterParsing(Text url, Content content, Parse parse) throws ScoringFilterException {
		parse.getData().getContentMeta().set(Nutch.SCORE_KEY, content.getMetadata().get(Nutch.SCORE_KEY));
	}
	
	public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) throws ScoringFilterException {
		content.getMetadata().set(Nutch.SCORE_KEY, "" + datum.getScore());
	}
	
	private float getScoreFromMetas(Text url, CrawlDatum datum){
		float PR = 0.0f, votes = 0.0f; 
		
	    org.apache.hadoop.io.MapWritable meta = datum.getMetaData();
	    FloatWritable pagerank = (FloatWritable)meta.get(new Text(Modification.META_PAGERANK));
	    
	    FloatWritable nb_votes = (FloatWritable)meta.get(new Text(Modification.META_VOTES));
	    if(pagerank != null) { PR = pagerank.get(); }
	    if(nb_votes != null) { votes = nb_votes.get(); }
	    
	    return PR + pagerankToVotesRatio*votes;
	    
		
	}
	
	public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum, List<CrawlDatum> inlinked) throws ScoringFilterException {
		LOG.debug("TrylogScoringFilter :: updateDbScore");
		if (old == null) old = datum;
		float newScore = getScoreFromMetas(url, datum);
	    LOG.debug("Trylog scoring filter new score : " + url + " -> " + newScore);
	    datum.setScore(newScore);
  }

}
