package com.github.amihalik.rya.mongo.debugging.linestring;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.github.amihalik.rya.mongo.debugging.RyaUtil;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class QueryData {

    private static final Logger log = Logger.getLogger(QueryData.class);

    public static void main(String[] args) throws Exception {

        log.info("Opening Connection to Rya");
        SailRepositoryConnection conn = RyaUtil.getSailRepo();
        log.info("Done Opening Connection to Rya");
        
        String query = Resources.toString(Resources.getResource("query.txt"), Charsets.UTF_8);
        
        TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        
        tq.evaluate(new SPARQLResultsJSONWriter(System.out));
    }


}
