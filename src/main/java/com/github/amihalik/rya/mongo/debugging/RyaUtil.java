package com.github.amihalik.rya.mongo.debugging;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

public class RyaUtil {
    private static final Logger log = Logger.getLogger(RyaUtil.class);
    
    private static String collectionName = "rya_";
    private static String tablePrefix = "rya_";
    private static String mongoInstance = "localhost";
    private static String mongoPort = "27017";
    private static String mongoDbName = "rya";
    private static boolean displayQueryPlan = true;

    public static SailRepositoryConnection getSailRepo() throws Exception {
        final Configuration conf = getConf();

        log.info("Connecting to Indexing Sail Repository.");
        final Sail extSail = GeoRyaSailFactory.getInstance(conf);
        SailRepository repository = new SailRepository(extSail);

        return repository.getConnection();
    }

    public static MongoDBRdfConfiguration getConf() {

        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setCollectionName(collectionName);
        conf.setTablePrefix(tablePrefix);
        conf.setMongoInstance(mongoInstance);
        conf.setMongoPort(mongoPort);;
        conf.setMongoDBName(mongoDbName);
        conf.setDisplayQueryPlan(displayQueryPlan);
        conf.setUseStats(false);
        
        conf.set("sc.useMongo", "true");
        conf.set("sc.use_geo", "true");
        conf.set("sc.geo.predicates", "http://www.opengis.net/ont/geosparql#asWKT");
 
        
        return conf;
    }
}
