package com.github.amihalik.rya.mongo.debugging.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.document.visibility.DocumentVisibilityAdapter;
import org.bson.Document;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

import com.mongodb.BasicDBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class MongoSerialization {
    private static final Logger LOG = Logger.getLogger(MongoSerialization.class);

    public static final String ID = "_id";
    public static final String OBJECT_TYPE = "objectType";

    public static final String OBJECT_TYPE_VALUE = XMLSchema.ANYURI.stringValue();
    public static final String CONTEXT = "context";
    public static final String PREDICATE = "predicate";
    public static final String PREDICATE_HASH = "predicate_hash_32";

    public static final String OBJECT = "object";
    public static final String OBJECT_HASH = "object_hash_32";

    public static final String SUBJECT = "subject";
    public static final String SUBJECT_HASH = "subject_hash_32";
    public static final String TIMESTAMP = "insertTimestamp";
    public static final String STATEMENT_METADATA = "statementMetadata";
    public static final String DOCUMENT_VISIBILITY = "documentVisibility";

    private static final String GEO = "location";
    
    private static final String EMPTY_METADATA = StatementMetadata.EMPTY_METADATA.toString();
    private static final List<?> EMPTY_VISIBILITY = new ArrayList<>();

    
    private final boolean addHash;
    private final boolean addGeo;
    
    public MongoSerialization(boolean addHash, boolean addGeo) {
        this.addHash = addHash;
        this.addGeo = addGeo;
    }
    
    public Document serialize(final Statement statement){
        String context = "";
        if (statement.getContext() != null){
            context = statement.getContext().stringValue();
        }
        String subject = statement.getSubject().stringValue();
        String predicate = statement.getPredicate().stringValue();
        String object ;
        String objectType;

        if (statement.getObject() instanceof URI) {
            object = statement.getObject().stringValue();
            objectType = OBJECT_TYPE_VALUE;
            
        } else {
            Literal lit = ((Literal)statement.getObject());
            object = lit.getLabel();
            objectType = lit.getDatatype().stringValue();

        }
        
        byte[] id_bytes = hash256(subject  + " " + predicate  + " " + object  + " " + context);

        
        final Document doc = new Document(ID, id_bytes)
            .append(SUBJECT, subject)
            .append(PREDICATE, predicate)
            .append(OBJECT, object)
            .append(OBJECT_TYPE, objectType)
            .append(CONTEXT, context)
            .append(STATEMENT_METADATA, EMPTY_METADATA)
            .append(DOCUMENT_VISIBILITY, EMPTY_VISIBILITY)
            .append(TIMESTAMP, System.currentTimeMillis());

        if (addHash) {
            doc.append(SUBJECT_HASH, hash32(subject));
            doc.append(PREDICATE_HASH, hash32(predicate));
            doc.append(OBJECT_HASH, hash32(object));
        }
        
        //append geo
        if (addGeo && objectType.equals("http://www.opengis.net/ont/geosparql#wktLiteral")) {
            try {
                Geometry geo = (new WKTReader()).read(object);
                if(geo == null) {
                    LOG.error("Failed to parse geo statement: " + statement.toString());
                    return null;
                }
                
                if (geo.getNumPoints() > 1) {
                    doc.append(GEO, getCorrespondingPoints(geo));
                } else {
                    doc.append(GEO, getDBPoint(geo));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return doc;
    }

    public static byte[] hash256(String str) {
        return DigestUtils.sha256(str);
    }

    public static byte[] hash32(String str) {
        return Arrays.copyOf(hash256(str), 4);
    }

    public static Document getCorrespondingPoints(final Geometry geo) {
        // Polygons must be a 3 dimensional array.

        // polygons must be a closed loop
        final Document geoDoc = new Document();
        if (geo instanceof Polygon) {
            final Polygon poly = (Polygon) geo;
            final List<List<List<Double>>> DBpoints = new ArrayList<>();

            // outer shell of the polygon
            final List<List<Double>> ring = new ArrayList<>();
            for (final Coordinate coord : poly.getExteriorRing().getCoordinates()) {
                ring.add(getPoint(coord));
            }
            DBpoints.add(ring);

            // each hold in the polygon
            for (int ii = 0; ii < poly.getNumInteriorRing(); ii++) {
                final List<List<Double>> holeCoords = new ArrayList<>();
                for (final Coordinate coord : poly.getInteriorRingN(ii).getCoordinates()) {
                    holeCoords.add(getPoint(coord));
                }
                DBpoints.add(holeCoords);
            }
            geoDoc.append("coordinates", DBpoints).append("type", "Polygon");
        } else {
            final List<List<Double>> points = getPoints(geo);
            geoDoc.append("coordinates", points).append("type", "LineString");
        }
        return geoDoc;
    }

    private static List<List<Double>> getPoints(final Geometry geo) {
        final List<List<Double>> points = new ArrayList<>();
        for (final Coordinate coord : geo.getCoordinates()) {
            points.add(getPoint(coord));
        }
        return points;
    }

    public static Document getDBPoint(final Geometry geo) {
        return new Document().append("coordinates", getPoint(geo)).append("type", "Point");
    }

    private static List<Double> getPoint(final Coordinate coord) {
        final List<Double> point = new ArrayList<>();
        point.add(coord.x);
        point.add(coord.y);
        return point;
    }

    private static List<Double> getPoint(final Geometry geo) {
        final List<Double> point = new ArrayList<>();
        point.add(geo.getCoordinate().x);
        point.add(geo.getCoordinate().y);
        return point;
    }

}
