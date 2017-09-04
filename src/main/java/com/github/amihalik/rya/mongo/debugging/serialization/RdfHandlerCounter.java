package com.github.amihalik.rya.mongo.debugging.serialization;

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class RdfHandlerCounter implements RDFHandler {
    private static final Logger log = Logger.getLogger(MongoSerialization.class);

    private RDFHandler inner;

    private int count = 0;
    private NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

    public RdfHandlerCounter(RDFHandler inner) {
        this.inner = inner;
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
        count++;
        if (count % 1_000_000 == 0) {
            log.info(numberFormat.format(count + " statements written"));
        }
        inner.handleStatement(st);

    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        inner.handleComment(comment);
    }
}
