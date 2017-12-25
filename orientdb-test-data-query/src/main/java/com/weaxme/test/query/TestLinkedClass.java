package com.weaxme.test.query;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class TestLinkedClass {

    private ODatabaseDocument db;

    public void create() {
        db = getDatabase();
        db.commit();
        String name = "Link";
        OClass test = db.getMetadata().getSchema().createClass("Test");
        test.createProperty("name", OType.STRING);
        OProperty link = test.createProperty("link", OType.LINK);
        link.setLinkedClass(createLinkedClass(name, db));
        db.begin();
        db.getMetadata().reload();
    }

    public void deleteLink() {
        db.commit();
        db.getMetadata().getSchema().getClass("Test").getProperty("link").setLinkedClass(null);
        db.getMetadata().reload();
        db.commit();
    }

    public OClass getPropertyLink() {
        return db.getMetadata().getSchema().getClass("Test").getProperty("link").getLinkedClass();
    }

    private OClass createLinkedClass(String name, ODatabaseDocument db) {
        return db.getMetadata().getSchema().createClass(name);
    }

    private ODatabaseDocument getDatabase() {
        ODatabaseDocument db = new ODatabaseDocumentTx("memory:test");
        if (!db.exists()) db.create();
        if (db.isClosed()) db.open("admin", "admin");
        return db;
    }
}
