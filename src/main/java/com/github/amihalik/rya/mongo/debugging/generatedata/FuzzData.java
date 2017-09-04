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

package com.github.amihalik.rya.mongo.debugging.generatedata;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

public class FuzzData {
    private static final Logger log = Logger.getLogger(FuzzData.class);

    private static final int MULTIPLIER = 12;

    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final WKTWriter ww = new WKTWriter();

    public static Statement fuzzStatement(Statement s, int id) throws Exception {
        if (id == 0) {
            return s;
        }

        URI subject = vf.createURI(s.getSubject().stringValue() + "_" + id);
        URI predicate = s.getPredicate();
        Value object = null;
        Value orgObject = s.getObject();

        if (orgObject instanceof URI) {
            object = vf.createURI(s.getObject().stringValue() + "_" + id);
        } else if (orgObject instanceof Literal) {
            Literal orgObjectLit = (Literal) orgObject;
            if (orgObjectLit.getDatatype().toString().equals("http://www.opengis.net/ont/geosparql#wktLiteral")) {
                Geometry geo = (new WKTReader()).read(orgObjectLit.getLabel());
                for (Coordinate c : geo.getCoordinates()) {
                    c.x += (Math.random() - .5);
                    c.y += (Math.random() - .5);
                }
                object = vf.createLiteral(ww.write(geo), orgObjectLit.getDatatype());
            } else {
                object = orgObject;
            }
        } else {
            object = orgObject;
            log.error("Unknown Obj Type :: " + orgObject);
        }

        return vf.createStatement(subject, predicate, object);
    }

    public static void main(String[] args) throws Exception {
        final RDFParser fileParser = Rio.createParser(RDFFormat.N3);
        //final RDFWriter fileWriter = Rio.createWriter(RDFFormat.BINARY, new BufferedOutputStream(new FileOutputStream("/mydata/one_gig_ntrip_file_12.brf")));
        
        fileParser.setRDFHandler(new RDFHandlerBase() {
            private int count = 0;
            private NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

            
            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                for (int i = 0; i < MULTIPLIER; i++) {
                    try {
                        count++;
                        if (count % 1_000_000 == 0) {
                            log.info(numberFormat.format(count + " statements written"));
                        }
                        fileWriter.handleStatement(fuzzStatement(st, i));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                fileWriter.endRDF();
                super.endRDF();
            }
        });

        fileParser.parse(FileUtils.openInputStream(new File("/mydata/one_gig_ntrip_file.n3")), "");

        log.info("Done writing data into file");

    }

}
