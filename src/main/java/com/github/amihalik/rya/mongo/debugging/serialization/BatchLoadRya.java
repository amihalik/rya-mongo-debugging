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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;

public class BatchLoadRya {
    private static final Logger log = Logger.getLogger(BatchLoadRya.class);

    private static final String DB_NAME = "rya_exp_3";
    private static final String COL_NAME = DB_NAME + "__triples";

    private static final String FILE_NAME = "/mydata/one_gig_ntrip_file_12.brf";

    private static final String HOST = "localhost";
    private static final int PORT = 27017;

    private static final int THREAD_COUNT = 4;

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

    private void loadintorya(final List<Statement> statements) {
        try {
            semaphore.acquire();
            executor.execute(() -> {
                MongoDatabase db = client.getDatabase(DB_NAME);
                final MongoCollection<Document> coll = db.getCollection(COL_NAME);

                List<Document> documents = new ArrayList<>();
                for (Statement s : statements) {
                    Document d = MongoSerialization.serialize(s);
                    documents.add(d);
                }
                coll.insertMany(documents, bws);
                int size = totalstatements.addAndGet(statements.size());
                System.out.println("TOTAL STATEMENTS ::" + size);
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
        client.close();
    }

    public static void main(String[] args) throws Exception {
        BatchLoadRya rya = new BatchLoadRya();
        RDFParser fileParser = Rio.createParser(RDFFormat.BINARY);
        fileParser.setRDFHandler(new RDFHandlerBase() {
            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                rya.loadStatement(st);
            }
            @Override
            public void endRDF() throws RDFHandlerException {
                rya.close();
            }
        });
        fileParser.parse(new BufferedInputStream(FileUtils.openInputStream(new File(FILE_NAME))), "");
        log.info("Done loading data into Rya");

    }

}
