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
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
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
import org.openrdf.rio.RDFHandler;
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

    public static void main(final String[] args) throws Exception {
        log.info("Opening Connection to Rya");
        final MongoDBRdfConfiguration config = RyaUtil.getConf();
        final MongoClient client = MongoConnectorFactory.getMongoClient(config);

        OptionalConfigUtils.setIndexers(config);
        final MongoDBRyaDAO dao = new MongoDBRyaDAO(config, client);
        dao.init();
        log.info("Done Opening Connection to Rya");

        log.info("Starting loading data into Rya");
        final RDFParser fileParser = Rio.createParser(RDFFormat.N3);

        final RDFHandler countingRdfHandler = new RDFHandlerBase() {
            private long rdfStartTime = 0L;
            private long batchStartTime = 0L;
            private final int batchSize = 100_000;

            private int batchCounter = 0;
            private long statementCounter = 0L;

            private final List<RyaStatement> statements = new ArrayList<>();

            @Override
            public void handleStatement(final Statement st) throws RDFHandlerException {
                statementCounter++;
                statements.add(RdfToRyaConversions.convertStatement(st));
                if (statements.size() == batchSize) {
                    loadBatchRya();
                }
            }

            @Override
            public void startRDF() throws RDFHandlerException {
                rdfStartTime = System.currentTimeMillis();
                batchStartTime = rdfStartTime;
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                loadBatchRya();

                // Final report
                final long endTime = System.currentTimeMillis();
                final long duration = endTime - rdfStartTime;
                final double tripPerSec = statementCounter / (duration / 1000);
                log.info("===============================================");
                log.info("Total Statements : " + statementCounter + " :: Total Batches : " + batchCounter + " :: Average Rate : " + Math.round(tripPerSec));
                log.info("Total Time Elapsed : " + DurationFormatUtils.formatDurationWords(duration, true, true));
            }

            private void loadBatchRya() throws RDFHandlerException {
                try {
                    batchCounter++;
                    final int currentBatchSize = statements.size();
                    log.info("Loading Batch #" + batchCounter + ".  Size : " + currentBatchSize);
                    dao.add(statements.iterator());

                    final long currentTime = System.currentTimeMillis();
                    final double tripPerSec = currentBatchSize / ((currentTime - batchStartTime) / 1000.);
                    log.info("Size : " + statementCounter + " :: Rate : " + Math.round(tripPerSec));
                    batchStartTime = currentTime;
                } catch (final RyaDAOException e) {
                    throw new RDFHandlerException(e);
                }
                statements.clear();
            }
        };

        fileParser.setRDFHandler(countingRdfHandler);

        final String filename = "/mydata/one_gig_ntrip_file.n3";
        try (final FileInputStream fin = FileUtils.openInputStream(new File(filename))) {
            fileParser.parse(fin, "");
        }

        log.info("Done loading data into Rya");

        // Allow things to flush out
        Thread.sleep(5000);

        dao.destroy();
    }
}
