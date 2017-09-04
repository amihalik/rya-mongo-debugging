package com.github.amihalik.rya.mongo.debugging.serialization;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import com.github.amihalik.rya.mongo.debugging.generatedata.FuzzData;

public class RdfHandlerFuzzer implements RDFHandler {
    private static final Logger log = Logger.getLogger(MongoSerialization.class);

    private RDFHandler inner;
    private int multiplier;

    public RdfHandlerFuzzer(RDFHandler inner, int multiplier) {
        this.inner = inner;
        this.multiplier = multiplier;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        inner.startRDF();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        inner.endRDF();
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        inner.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        for (int i = 0; i < multiplier; i++) {
            try {
                inner.handleStatement(FuzzData.fuzzStatement(st, i));
            } catch (Exception e) {
                log.error("Error fuzzing statement", e);
                throw new RDFHandlerException(e);
            }
        }

    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        inner.handleComment(comment);
    }
}
