package com.argakon.solr;

import org.apache.lucene.queries.function.*;
import org.apache.lucene.index.LeafReaderContext;


import java.util.Map;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.solr.schema.FieldType;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

/**
 *
 * @author argakon
 */
public class RedisValueSource extends ValueSource {
	protected String redisKey;
	protected String field;
	protected FieldType fType;

	protected float defVal;

	protected String host;
	protected int port;
	protected String password;
	protected int timeout;
	protected int scanCount;
	protected String redisDataType;

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	public RedisValueSource(String host, int port, String password, int timeout, int scanCount, 
			String redisDataType, String redisKey, String field, float defVal, FieldType fType) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.timeout = timeout;
		this.scanCount = scanCount;
		this.redisDataType = redisDataType;

		this.redisKey = redisKey;
		this.field = field;
		this.defVal = defVal;
		this.fType = fType;
	}

	@Override
	public String description() {
		return "Redis key value external storage source";
	}

	@Override
	public int hashCode() {
		return RedisValueSource.class.hashCode() + fType.hashCode();

	}

	@Override
	public String toString() {
		return "RedisValueSource(host" + host + ",port=" + port + ",password=" + password +
				",timeout=" + timeout +
				",scanCount=" + scanCount +
				",redisDataType=" + redisDataType +
				",field=" + field +
				",redisKey=" + redisKey +
				",defVal=" + defVal + ")";

	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass() != RedisValueSource.class) return false;
		final RedisValueSource other = (RedisValueSource) o;
		return this.host.equals(other.host)
			&& this.port == other.port
			&& this.password == other.password
			&& this.timeout == other.timeout
			&& this.scanCount == other.scanCount
			&& this.redisDataType == other.redisDataType
			&& this.field.equals(other.field)
            && this.redisKey.equals(other.redisKey)
            && this.defVal == other.defVal;
	}

	@Override
	public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
		final int off = readerContext.docBase;
		
		IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);
		final float[] arr_f = getCachedFloats(topLevelContext.reader());

		FunctionValues result = new FunctionValues() {
			@Override 
			public String strVal(int doc) {
				return new String(Float.toString(floatVal(doc)));
			}

			@Override
			public double doubleVal(int doc) {
				return (double) floatVal(doc);
			}

			@Override
			public float floatVal(int doc) {
				return arr_f[doc + off];
			}

			@Override
			public String toString(int doc) {
				return strVal(doc);
			}
		};

		return result;
	}

	private static float[] getFloats(RedisValueSource rvs, IndexReader reader) {
		float[] vals = new float[reader.maxDoc()];
		if (rvs.defVal != 0) {
			Arrays.fill(vals, rvs.defVal);
		}
		String idName = rvs.field;

		BytesRefBuilder internalKey = new BytesRefBuilder();
		try {
			TermsEnum termsEnum = MultiFields.getTerms(reader, idName).iterator();
			PostingsEnum postingsEnum = null;

			Jedis jedis = new Jedis(rvs.host, rvs.port, rvs.timeout);
			if (rvs.password.length() > 0)
				if (!jedis.auth(rvs.password).equals("OK"))
					log.error("Can't authenticate with specified password");

			if (rvs.redisDataType.equalsIgnoreCase("z")) {
				long startIndex = 0;
				boolean rangeFinished = false;
				while (!rangeFinished) {
					Set<Tuple> result = jedis.zrangeWithScores(rvs.redisKey, startIndex, startIndex + rvs.scanCount);
					if (result.isEmpty() || result.size() < rvs.scanCount)
						rangeFinished = true;

					for(Tuple tuple: result) {
						String key = tuple.getElement();
						float fval = (float) tuple.getScore();
						rvs.fType.readableToIndexed(key, internalKey);

						if (!termsEnum.seekExact(internalKey.get())) {
							continue;
						}

						postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
						int doc;
						while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
							vals[doc] = fval;
						}
					}

					startIndex += rvs.scanCount;
				}
			} else if (rvs.redisDataType.equalsIgnoreCase("h")) {
				// TODO: Not implemented yet
			} else if (rvs.redisDataType.equalsIgnoreCase("k")) {
				// regular keys, so we cast redisKey as prefix for field and use scan on keys
				// be careful, on large amount of keys this method can be very slow

				String cur = ScanParams.SCAN_POINTER_START;
				boolean rangeFinished = false;
				while (!rangeFinished) {
					ScanResult<String> scanResult = jedis.scan(cur, new ScanParams().count(rvs.scanCount));
					List<String> redisKeys = scanResult.getResult();

					for(String prefixedKey: redisKeys) {
						int delimIndex = prefixedKey.lastIndexOf(':');
						if (delimIndex < 0) continue;

						String prefix = prefixedKey.substring(0, delimIndex);
						if (!prefix.equals(rvs.redisKey)) continue; // wrong prefix

						String val = jedis.get(prefixedKey);
						if (val == null) continue;
						float fval = Float.parseFloat(val);

						int endIndex = prefixedKey.length();
						String key = prefixedKey.substring(delimIndex + 1, endIndex);

						rvs.fType.readableToIndexed(key, internalKey);
						if (!termsEnum.seekExact(internalKey.get())) {
							continue;
						}

						postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
						int doc;
						while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
							vals[doc] = fval;
						}
					}

					cur = scanResult.getStringCursor();
					if (cur.equals("0")) {
						rangeFinished = true;
					}
				}
			}
			
			if (jedis.isConnected())
				jedis.disconnect();

		}catch (IOException e) {
			// log, use defaults
			log.error("Error loading Redis value source: " + e);
		}


		return vals;
	}

	private final float[] getCachedFloats(IndexReader reader) {
		return (float[]) floatCache.get(reader, new Entry(this));
	}

	/**
	* Remove all cached entries.  Values are lazily loaded next time getValues() is
	* called.
	*/
	public static void resetCache(){
		floatCache.resetCache();
	}

	/**
	 * Refresh the cache for an IndexReader.  The new values are loaded in the background
	 * and then swapped in, so queries against the cache should not block while the reload
	 * is happening.
	 * @param reader the IndexReader whose cache needs refreshing
	 */
	public void refreshCache(IndexReader reader) {
		log.info("Refreshing RedisValueSource cache for field {}", this.field);
		floatCache.refresh(reader, new Entry(this));
		log.info("RedisValueSource cache for field {} reloaded", this.field);
	}
	
	static Cache floatCache = new Cache() {
		@Override
		protected Object createValue(IndexReader reader, Object key) {
			return getFloats(((Entry)key).rvs, reader);
		}
	};
	
	  /** Expert: Every composite-key in the internal cache is of this type. */
	private static class Entry {
		final RedisValueSource rvs;
		public Entry(RedisValueSource rvs) {
			this.rvs = rvs;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Entry)) return false;
			Entry other = (Entry)o;
			return rvs.equals(other.rvs);
		}

		@Override
		public int hashCode() {
			return rvs.hashCode();
		}
	}
	static final class CreationPlaceholder {
		Object value;
	}

	/** Internal cache. (from lucene FieldCache) */
	abstract static class Cache {
	  private final Map readerCache = new WeakHashMap();

	  protected abstract Object createValue(IndexReader reader, Object key);

	  public void refresh(IndexReader reader, Object key) {
		Object refreshedValues = createValue(reader, key);
		synchronized (readerCache) {
		  Map innerCache = (Map) readerCache.get(reader);
		  if (innerCache == null) {
			innerCache = new HashMap();
			readerCache.put(reader, innerCache);
		  }
		  innerCache.put(key, refreshedValues);
		}
	  }

	  public Object get(IndexReader reader, Object key) {
		Map innerCache;
		Object value;
		synchronized (readerCache) {
		  innerCache = (Map) readerCache.get(reader);
		  if (innerCache == null) {
			innerCache = new HashMap();
			readerCache.put(reader, innerCache);
			value = null;
		  } else {
			value = innerCache.get(key);
		  }
		  if (value == null) {
			value = new CreationPlaceholder();
			innerCache.put(key, value);
		  }
		}
		if (value instanceof CreationPlaceholder) {
		  synchronized (value) {
			CreationPlaceholder progress = (CreationPlaceholder) value;
			if (progress.value == null) {
			  progress.value = createValue(reader, key);
			  synchronized (readerCache) {
				innerCache.put(key, progress.value);
			  }
			}
			return progress.value;
		  }
		}

		return value;
	  }

	  public void resetCache(){
		synchronized(readerCache){
		  // Map.clear() is optional and can throw UnsupportedOperationException,
		  // but readerCache is WeakHashMap and it supports clear().
		  readerCache.clear();
		}
	  }
  }

	public static class ReloadCacheRequestHandler extends RequestHandlerBase {
		@Override
		public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
			RedisValueSource.resetCache();
			log.debug("readerCache has been reset.");

			UpdateRequestProcessor processor =
				req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
			try {
				RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
			} finally {
				processor.finish();
			}
		}

		@Override
		public String getDescription() {
			return "Reload readerCache request handler";
		}
	}
}
