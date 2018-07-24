# SolrRedisExternalField

### Note: Plugin acts like ExternalFileField

After `git clone`, run `mvn build`, put compiled jar in Solr class path.

Add new field type in managed-schema
```xml
 <fieldType name="dealsRedis" keyField="id" 
    redisKey="deals:vehicles:cars" 
    defVal="0" 
    stored="true" 
    indexed="true"
    class="com.argakon.solr.RedisField" 
    valType="float" 
    host="127.0.0.1" 
    port="6379" 
    password="password" 
    scanCount="3000" 
    dataType="z" />
```

Add new listeners in solrconfig.xml for cache heat. On big collections can take a long time :(.
```xml
<!-- Redis external data reloader -->
<listener event="newSearcher" class="com.argakon.solr.RedisFieldReloader"/>
<listener event="firstSearcher" class="com.argakon.solr.RedisFieldReloader"/>
```

If you want to use RedisField values in bq you can add this:
```xml
<valueSourceParser name="deals" class="com.argakon.solr.RedisValueSourceParser">
      <str name="host">127.0.0.1</str>
      <str name="port">6379</str>
      <str name="password">password</str>
      <str name="timeout">60000</str>
      <str name="scanCount">1000</str>
      <str name="dataType">z</str>
      <str name="redisKey">deals</str>
      <str name="defVal">0</str>
</valueSourceParser>
```
After that simply add `deals()` to `bf`
