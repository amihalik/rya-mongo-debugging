package com.github.amihalik.rya.mongo.debugging.linestring;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.github.amihalik.rya.mongo.debugging.RyaUtil;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.io.WKTWriter;

public class LoadLineString {

    private static final Logger log = Logger.getLogger(LoadLineString.class);

    public static void main(String[] args) throws Exception {
        int pointsInLineString = 20;

        LineString one_zero = createCircleLineString(new Coordinate(100, 100), 35, pointsInLineString);
        LineString one_five = createCircleLineString(new Coordinate(100, 150), 35, pointsInLineString);
        LineString two_zero = createCircleLineString(new Coordinate(100, 200), 35, pointsInLineString);
        LineString two_five = createCircleLineString(new Coordinate(100, 250), 35, pointsInLineString);
        LineString three_zero = createCircleLineString(new Coordinate(100, 300), 35, pointsInLineString);

//        Geopanel.createGeoFrame(one_zero, one_five, two_zero, two_five, three_zero);
//
//        log.info("Opening Connection to Rya");
//        SailRepositoryConnection conn = RyaUtil.getSailRepo();
//        log.info("Done Opening Connection to Rya");
//
//        log.info("Starting loading data into Rya");
//        Statement s;
//        s = createStatement("one_zero", one_zero);
//        log.info("Loading Statement :: " + s);
//        conn.add(s);
//
//        s = createStatement("two_zero", two_zero);
//        log.info("Loading Statement :: " + s);
//        conn.add(s);
//
//        s = createStatement("three_zero", three_zero);
//        log.info("Loading Statement :: " + s);
//        conn.add(s);
//
//        log.info("Done loading data into Rya");
//        
//        conn.close();
        System.out.println(new Coordinate(100, 100));
    }

    public static Statement createStatement(String name, Geometry geo) {
        WKTWriter w = new WKTWriter();
        String wkt = w.write(geo);
        
        ValueFactory vf = new ValueFactoryImpl();
        URI subject = vf.createURI("s:" + name);
        URI predicate = vf.createURI("http://www.opengis.net/ont/geosparql#asWKT");
        
        URI wktType = vf.createURI("http://www.opengis.net/ont/geosparql#wktLiteral");
        Literal object = vf.createLiteral(wkt, wktType);
        
        return vf.createStatement(subject, predicate, object);
    }

    public static LineString createCircleLineString(Coordinate center, double radius, int pointCount) {
        Coordinate[] coords = new Coordinate[pointCount];
        for (int i = 0; i < pointCount; i++) {
            double x = radius * Math.cos(2. * Math.PI * i / (pointCount - 1)) + center.x;
            double y = radius * Math.sin(2. * Math.PI * i / (pointCount - 1)) + center.y;
            coords[i] = new Coordinate(x, y);
        }
        return new GeometryFactory().createLineString(coords);

    }

}
