package com.argakon.solr;


import org.apache.solr.search.FunctionQParser;
import org.apache.lucene.queries.function.ValueSource;
import com.bbs.solr.RedisValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

/**
 *
 * @author argakon
 */
public class RedisValueSourceParser extends ValueSourceParser {

	private String host;
	private int port;
	private String password;
	private int timeout;
	private int scanCount;
	private String dataType;

	private String redisKey;
	private float defVal;

	@Override
	public void init(NamedList args) {
		String hostS = (String) args.get("host");
		host = hostS == null ? "localhost" : hostS;

		String portS = (String) args.get("port");
		port = portS == null ? 6379 : Integer.parseInt(portS);

		String passwordS = (String) args.get("password");
		password = passwordS == null ? "" : passwordS;

		String timeoutS = (String) args.get("timeout");
		timeout = timeoutS == null ? 60000 : Integer.parseInt(timeoutS);

		String scanCountS = (String) args.get("scanCount");
		scanCount = scanCountS == null ? 1000 : Integer.parseInt(scanCountS);

		String dataTypeS = (String) args.get("dataType");
		dataType = dataTypeS == null ? "" : dataTypeS;

		String redisKeyS = (String) args.get("redisKey");
		if (redisKeyS == null || redisKeyS.isEmpty())
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
					"Redis key must be specified");
		redisKey = redisKeyS;

		String defValS = (String) args.get("defVal");
		defVal = defValS == null ? 0 : Float.parseFloat(defValS);
	}

	@Override
	public ValueSource parse(FunctionQParser fp) throws SyntaxError {
		String field = fp.parseArg();

		FieldType ft = fp.getReq().getSchema().getFieldTypeNoEx(field);
		if (ft == null) ft = new StrField();

		return new RedisValueSource(host, port, password, timeout, scanCount, dataType, redisKey, field, defVal, ft);
	}
}
