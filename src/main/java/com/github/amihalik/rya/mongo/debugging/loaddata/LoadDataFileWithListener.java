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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.RDFHandlerWrapper;

import com.github.amihalik.rya.mongo.debugging.RyaUtil;

/**
 * Uses the sail layer to load a N3 file. Prints out the loading progress to the
 * log.
 */
public class LoadDataFileWithListener {
    private static final Logger log = Logger.getLogger(LoadDataFileWithListener.class);

    public static void main(final String[] args) throws Exception {
        log.info("Opening Connection to Rya");
        final SailRepositoryConnection conn = RyaUtil.getSailRepo();
        log.info("Done Opening Connection to Rya");

        log.info("Starting loading data into Rya");
        final RDFParser fileParser = Rio.createParser(RDFFormat.N3);

        final RDFInserter rdfInserter = new RDFInserter(conn);

        final RDFHandler countingRdfHandler = new RDFHandlerBase() {
            private long rdfStartTime = 0L;
            private long batchStartTime = 0L;
            private final int batchSize = 100_000;

            private int batchCounter = 0;
            private long statementCounter = 0L;

            @Override
            public void handleStatement(final Statement st) throws RDFHandlerException {
                statementCounter++;
                if (statementCounter % batchSize == 0) {
                    batchCounter++;
                    log.info("Loading Batch #" + batchCounter + ".  Size : " + batchSize);

                    final long currentTime = System.currentTimeMillis();
                    final double tripPerSec = batchSize / ((currentTime - batchStartTime) / 1000.);
                    log.info("Size : " + statementCounter + " :: Rate : " + Math.round(tripPerSec));
                    batchStartTime = currentTime;
                }
            }

            @Override
            public void startRDF() throws RDFHandlerException {
                rdfStartTime = System.currentTimeMillis();
                batchStartTime = rdfStartTime;
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                final long remainingStatements = statementCounter - (batchSize * batchCounter);
                if (remainingStatements > 0) {
                    batchCounter++;
                    log.info("Loading Batch #" + batchCounter + ".  Size : " + remainingStatements);

                    final long currentTime = System.currentTimeMillis();
                    final double tripPerSec = remainingStatements / ((currentTime - batchStartTime) / 1000.);
                    log.info("Size : " + statementCounter + " :: Rate : " + Math.round(tripPerSec));
                    batchStartTime = currentTime;
                }

                // Final report
                final long endTime = System.currentTimeMillis();
                final long duration = endTime - rdfStartTime;
                final double tripPerSec = statementCounter / (duration / 1000);
                log.info("===============================================");
                log.info("Total Statements : " + statementCounter + " :: Total Batches : " + batchCounter + " :: Average Rate : " + Math.round(tripPerSec));
                log.info("Total Time Elapsed : " + DurationFormatUtils.formatDurationWords(duration, true, true));
            }
        };

        fileParser.setRDFHandler(new RDFHandlerWrapper(rdfInserter, countingRdfHandler));

        final String filename = "/mydata/one_gig_ntrip_file.n3";
        try (final FileInputStream fin = FileUtils.openInputStream(new File(filename))) {
            fileParser.parse(fin, "");
        }

        log.info("Done loading data into Rya");

        // Allow things to flush out
        Thread.sleep(5000);

        conn.close();
    }
}
