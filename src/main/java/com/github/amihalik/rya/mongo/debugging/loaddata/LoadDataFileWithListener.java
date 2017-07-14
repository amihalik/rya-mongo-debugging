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

import org.apache.commons.io.FileUtils;
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
 * Uses the sail layer to load a N3 file.  Prints out the loading progress to the log.
 */
public class LoadDataFileWithListener {
    private static final Logger log = Logger.getLogger(LoadDataFileWithListener.class);



    public static void main(String[] args) throws Exception {
        log.info("Opening Connection to Rya");
        SailRepositoryConnection conn = RyaUtil.getSailRepo();
        log.info("Done Opening Connection to Rya");
        
        log.info("Starting loading data into Rya");
        RDFParser fileParser = Rio.createParser(RDFFormat.N3);
        
        RDFInserter rdfInserter = new RDFInserter(conn);

        RDFHandler countingRdfHandler = new RDFHandlerBase() {
            
            private int count = 0;

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                count++;
                if (count % 100_000 == 0) {
                    log.info("Size :: " + count);
                }
            }
        };            

        
        fileParser.setRDFHandler(new RDFHandlerWrapper(rdfInserter, countingRdfHandler));
        
        fileParser.parse(FileUtils.openInputStream(new File("/mydata/one_gig_ntrip_file.n3")), "");
        log.info("Done loading data into Rya");

    }
}
