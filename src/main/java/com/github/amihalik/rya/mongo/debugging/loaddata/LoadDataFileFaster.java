/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.amihalik.rya.mongo.debugging.loaddata;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.OptionalConfigUtils;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.github.amihalik.rya.mongo.debugging.RyaUtil;
import com.mongodb.MongoClient;

/**
 * Uses the DAO layer to load a N3 file. Two issues with using the DAO:
 * 
 * (1) It's a bit more complicated to set up the DAO and this "batch" loading
 * method
 * 
 * (2) The DAO has bad error handling. Basically, if there is an error inserting
 * one triple, the entire load may break.
 * 
 * However, this seems to be significantly faster. I'm getting 10k trip/sec
 * using the DAO and 2k trip/sec using the sail layer.
 */
public class LoadDataFileFaster {
    private static final Logger log = Logger.getLogger(LoadDataFileFaster.class);

    public static void main(String[] args) throws Exception {
        log.info("Opening Connection to Rya");
        MongoDBRdfConfiguration config = RyaUtil.getConf();
        final MongoClient client = MongoConnectorFactory.getMongoClient(config);

        OptionalConfigUtils.setIndexers(config);
        MongoDBRyaDAO dao = new MongoDBRyaDAO(config, client);
        dao.init();
        log.info("Done Opening Connection to Rya");

        log.info("Starting loading data into Rya");
        RDFParser fileParser = Rio.createParser(RDFFormat.N3);

        fileParser.setRDFHandler(new RDFHandlerBase() {
            private long startTime = System.currentTimeMillis();
            private final int batchSize = 100_000;

            private final List<RyaStatement> statements = new ArrayList<>();

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                statements.add(RdfToRyaConversions.convertStatement(st));
                if (statements.size() == batchSize) {
                    loadBatchRya();
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                loadBatchRya();
            }

            private void loadBatchRya() throws RDFHandlerException {
                try {
                    log.info("Loading Batch.  Size : " + statements.size());
                    dao.add(statements.iterator());

                    long currentTime = System.currentTimeMillis();
                    double tripPerSec = batchSize / ((currentTime - startTime) / 1000.);
                    log.info("Size : " + batchSize + " :: Rate : " + Math.round(tripPerSec));
                    startTime = currentTime;

                } catch (RyaDAOException e) {
                    throw new RDFHandlerException(e);
                }
                statements.clear();
            }
        });

        fileParser.parse(FileUtils.openInputStream(new File("/mydata/one_gig_ntrip_file.n3")), "");
        log.info("Done loading data into Rya");

    }
}
