package com.github.amihalik.rya.mongo.debugging.linestring;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Shape;

import javax.swing.JFrame;
import javax.swing.JPanel;

import com.vividsolutions.jts.awt.ShapeWriter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

public class Geopanel extends JPanel {

    private Geometry[] geometries;

    public Geopanel(Geometry... geometries) {
        this.geometries = geometries;
    }

    @Override
    public void paint(Graphics g) {
        ShapeWriter sw = new ShapeWriter();

        for (Geometry geo : geometries) {
            ((Graphics2D) g).draw(sw.toShape(geo));
        }

    }

    public static void createGeoFrame(Geometry...geometries ) {
        JFrame f = new JFrame();
        f.getContentPane().add(new Geopanel(geometries));
        f.setSize(700, 700);
        f.setVisible(true);
    }
    
    public static void main(String[] args) {
        Coordinate[] coords = new Coordinate[] { new Coordinate(400, 0), new Coordinate(200, 200), new Coordinate(400, 400),
                new Coordinate(600, 200), new Coordinate(400, 0) };
        Polygon polygon = new GeometryFactory().createPolygon(coords);

        LineString ls = new GeometryFactory().createLineString(new Coordinate[] { new Coordinate(20, 20), new Coordinate(200, 20) });
        createGeoFrame(polygon, ls);
    }
}
