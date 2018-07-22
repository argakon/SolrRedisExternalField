package com.argakon.solr;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.search.QParser;

import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 *
 * @author argakon
 */
public class RedisField extends FieldType implements SchemaAware {
	private FieldType ftype;
	private String keyFieldName;
	private IndexSchema schema;
	private String redisKey;
	private float defVal;

	private String host;
	private int port;
	private String password;
	private int timeout;
	private int scanCount;
	private String dataType;

	@Override
	protected void init(IndexSchema schema, Map<String, String> args) {
		restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
		// valType has never been used for anything except to throw an error, so make it optional since the
		// code (see getValueSource) gives you a RedisValueSource.
		String ftypeS = args.remove("valType");
		if (ftypeS != null) {
		  ftype = schema.getFieldTypes().get(ftypeS);
		  if (ftype != null && !(ftype instanceof TrieFloatField)) {
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
				"Only float (TrieFloatField) is currently supported as external field type.  Got " + ftypeS);
		  }
		}
		String hostS = args.remove("host");
		host = hostS == null ? "localhost" : hostS;

		String portS = args.remove("port");
		port = portS == null ? 6379 : Integer.parseInt(portS);

		String passwordS = args.remove("password");
		password = passwordS == null ? "" : passwordS;

		String timeoutS = args.remove("timeout");
		timeout = timeoutS == null ? 60000 : Integer.parseInt(timeoutS);

		String scanCountS = args.remove("scanCount");
		scanCount = scanCountS == null ? 1000 : Integer.parseInt(scanCountS);

		String dataTypeS = (String) args.remove("dataType");
		dataType = dataTypeS == null ? "" : dataTypeS;
		if (!dataType.equalsIgnoreCase("z") && !dataType.equalsIgnoreCase("h") && !dataType.equalsIgnoreCase("k"))
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
				"Wrong redis data type, only sorted set or hash or regular keys are supported");
		
		redisKey = args.remove("redisKey");
		if (redisKey == null || redisKey.length() < 1) {
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
				"Redis Sorted Set or hash key must be set");
		}

		keyFieldName = args.remove("keyField");

		String defValS = args.remove("defVal");
		defVal = defValS == null ? 0 : Float.parseFloat(defValS);

		this.schema = schema;
	}

	@Override
	public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortField getSortField(SchemaField field, boolean reverse) {
		RedisValueSource source = getRedisValueSource(field);
		return source.getSortField(reverse);
	}

	@Override
	public Type getUninversionType(SchemaField sf) {
		return null;
	}

	@Override
	public ValueSource getValueSource(SchemaField field, QParser parser) {
		return getRedisValueSource(field);
	}

	public RedisValueSource getRedisValueSource(SchemaField field) {
		// Because the redis source uses a static cache, all source objects will
		// refer to the same data.
		return new RedisValueSource(host, port, password, timeout, scanCount, dataType,
				redisKey, keyFieldName, defVal, getKeyField().getType());
	}

	private SchemaField getKeyField() {
		return keyFieldName == null ?
			schema.getUniqueKeyField() :
			schema.getField(keyFieldName);
	}

	@Override
	public void inform(IndexSchema schema) {
		this.schema = schema;
	}
}
