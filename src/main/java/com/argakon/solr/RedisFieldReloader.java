package com.argakon.solr;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *
 * @author argakon
 */
public class RedisFieldReloader extends AbstractSolrEventListener {
	
	private List<RedisValueSource> fieldSources = new ArrayList<>();

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	public RedisFieldReloader(SolrCore core) {
		super(core);
	}

	@Override
	public void init(NamedList args) {
		cacheFieldSources(getCore().getLatestSchema());
	}

	@Override
	public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
		// We need to reload the caches for the new searcher
		if (null == currentSearcher || newSearcher.getSchema() != currentSearcher.getSchema()) {
			cacheFieldSources(newSearcher.getSchema());
		}
		IndexReader reader = newSearcher.getIndexReader();
		for (RedisValueSource fieldSource : fieldSources) {
			fieldSource.refreshCache(reader);
		}
	}

	/** Caches RedisValueSource's from all RedisField instances in the schema */
	public void cacheFieldSources(IndexSchema schema) {
		fieldSources.clear();
		for (SchemaField field : schema.getFields().values()) {
			FieldType type = field.getType();
			if (type instanceof RedisField) {
				RedisField rf = (RedisField)type;
				fieldSources.add(rf.getRedisValueSource(field));
				log.info("Adding RedisFieldReloader listener for field {}", field.getName());
			}
		}
	}
}
