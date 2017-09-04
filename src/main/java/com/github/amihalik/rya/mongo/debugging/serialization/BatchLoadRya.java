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

package com.github.amihalik.rya.mongo.debugging.serialization;

import java.io.BufferedInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;

public class BatchLoadRya {
    private static final Logger log = Logger.getLogger(BatchLoadRya.class);

    private static final String DB_NAME = "rya_exp";
    private static final String COL_NAME = DB_NAME + "__triples";

    private static final String HOST = "localhost";
    private static final int PORT = 27017;

    private static final int THREAD_COUNT = 2;

    private final int BATCH_SIZE = 1_000_000;

    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    private final Semaphore semaphore = new Semaphore(THREAD_COUNT);

    private final InsertManyOptions bws;
    final AtomicInteger totalstatements = new AtomicInteger();

    final MongoClient client;

    public BatchLoadRya() throws Exception {
        log.info("Opening Connection to Rya");

        ServerAddress server = new ServerAddress(HOST, PORT);
        client = new MongoClient(server, MongoClientOptions.builder().minConnectionsPerHost(THREAD_COUNT).build());

        bws = new InsertManyOptions();
        bws.ordered(false);

        log.info("Done Opening Connection to Rya");
    }

    private final List<Statement> statements = new ArrayList<>();

    public void loadStatement(Statement s) {
        statements.add(s);
        if (statements.size() >= BATCH_SIZE) {
            loadintorya(new ArrayList<>(statements));
            statements.clear();
        }
    }

    private void loadintorya(final List<Statement> sts) {
        try {
            semaphore.acquire();
            executor.execute(() -> {
                MongoDatabase db = client.getDatabase(DB_NAME);
                final MongoCollection<Document> coll = db.getCollection(COL_NAME);

                Stopwatch sw = new Stopwatch();
                sw.start();
                List<Document> documents = new ArrayList<>();
                for (Statement s : sts) {
                    Document d = MongoSerialization.serialize(s);
                    documents.add(d);
                }

                long serMilli = sw.elapsed(TimeUnit.MILLISECONDS);

                sw.reset();
                sw.start();
                try {
                    coll.insertMany(documents, bws);
                    long insertMilli = sw.elapsed(TimeUnit.MILLISECONDS);

                    int totalsize = totalstatements.addAndGet(sts.size());
                    int insertsize = sts.size();
                    log.info(String.format("TOTAL STATEMENTS :: %,d\t Serialization Rate :: %,d\t Insert Rate :: %,d\t", totalsize,
                            (int) (insertsize * 1000. / serMilli), (int) (insertsize * 1000. / insertMilli)));
                } catch (MongoBulkWriteException e) {
                    log.error("Bulk Write Error loading data into Mongo.  First Message :: " + e.getWriteErrors().get(0).getMessage() + ". "
                            + e.getWriteErrors().size() + " total errors");
                } catch (MongoException e) {
                    log.error("Error loading data into Mongo");
                    e.printStackTrace();
                }

                semaphore.release();
            });
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void close() {
        loadintorya(new ArrayList<>(statements));
        statements.clear();
        // wait until all threads have finished writing
        try {
            semaphore.acquire(THREAD_COUNT);
            client.close();
        } catch (InterruptedException e) {
            log.error("Mongo Client not closed properly");
            e.printStackTrace();
        }
    }

    public static RDFHandler newHandler() throws Exception {
        BatchLoadRya rya = new BatchLoadRya();
        return new RDFHandlerBase() {
            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                rya.loadStatement(st);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                rya.close();
            }
        };
    }

    public static void main(String[] args) throws Exception {
        RDFParser fileParser = Rio.createParser(RDFFormat.N3);
        
        RDFHandler rya = BatchLoadRya.newHandler();
        RDFHandler counter = new RdfHandlerCounter(rya);
        RDFHandler fuzzer = new RdfHandlerFuzzer(rya, 12);

        fileParser.setRDFHandler(fuzzer);
        
        fileParser.parse(new BufferedInputStream(FileUtils.openInputStream(new File(FILE_NAME))), "");

        
        
        log.info("Done loading data into Rya");

    }

}
