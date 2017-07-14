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

import org.apache.log4j.Logger;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.github.amihalik.rya.mongo.debugging.RyaUtil;

/**
 * Uses the sail layer to load a N3 file.
 */
public class LoadDataFile {
    private static final Logger log = Logger.getLogger(LoadDataFile.class);



    public static void main(String[] args) throws Exception {
        log.info("Opening Connection to Rya");
        SailRepositoryConnection conn = RyaUtil.getSailRepo();
        log.info("Done Opening Connection to Rya");
        
        log.info("Starting loading data into Rya");
        conn.add(new File("/mydata/one_gig_ntrip_file.n3"), null, RDFFormat.N3);
        log.info("Done loading data into Rya");

    }
}
