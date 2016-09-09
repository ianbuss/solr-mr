# Simple Secure Solr MR Example
  
Example code for managing connections to Kerberos secured Solr servers from MapReduce tasks.

## Build

```
mvn clean package
```

## Use

```
yarn jar solr-mr-<version>.jar SimpleSolrMRIndexer <hdfs_input_dir> <zookeeper_connection_string> <collection_name> [<jaas_file>]
```

For example:

```
yarn jar solr-mr-1.0-SNAPSHOT-jar-with-dependencies.jar SimpleSolrMRIndexer solrtest host1.ib.dev:2181/solr coll2 jaas.conf
```

## Example JAAS file

```
Client {
 com.sun.security.auth.module.Krb5LoginModule required
 useKeyTab=false
 useTicketCache=true
 principal="vagrant@IB.DEV";
};
```
