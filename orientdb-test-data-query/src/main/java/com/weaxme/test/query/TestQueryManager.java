package com.weaxme.test.query;


import com.google.common.collect.Lists;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class TestQueryManager {
    private static final Logger LOG = LoggerFactory.getLogger(TestQueryManager.class);

    private static final String DATABASE_NAME = "plocal:/tmp/database/showAllDocuments-query-db";

    public void showAllDocuments() {
        ODatabaseDocument db = getDatabase();

        try {
            LOG.info("Test creating database. Show documents in database.");
            for (OClass oClass : db.getMetadata().getSchema().getClasses()) {
                LOG.info("Documents of class: {}", oClass.getName());
                for (ODocument doc : db.browseClass(oClass.getName())) {
                    LOG.info("document: {}", doc);
                }
            }
        } finally {
            db.close();
        }
    }

    public ODocument createDocumentField(String className, String fieldName, OType fieldType, String data) {
        ODatabaseDocument db = getDatabase();
        OClass oClass = getOClass(className, db.getMetadata().getSchema(), fieldName, fieldType);
        ODocument document;
        try {
            document = new ODocument(oClass);
            document.field(fieldName, data);
            document.save();
        } finally {
            db.commit();
            db.close();
        }
        return document;
    }

    public void showOClassDocuments(String className) {
        ODatabaseDocument db = getDatabase();
        if (db.getMetadata().getSchema().existsClass(className)) {
            LOG.info("Documents of OClass {}", className);
            for (ODocument doc : db.browseClass(className)) {
                LOG.info("document: {}", doc);
            }
        } else {
            LOG.warn("OClass with name {} don't exist in database", className);
        }
    }

    public List<ODocument> executeQuery(String sql) {
        ODatabaseDocument db = getDatabase();
        List<ODocument> result;
        try {
            LOG.info("Execute SQL query:  {}", sql);
            OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(sql);
            result = db.query(query);
        } finally {
            db.close();
        }
        return result != null ? result : Lists.<ODocument>newArrayList();
    }

    public void clear() {
        getDatabase().drop();
    }

    private ODatabaseDocument getDatabase() {
        ODatabaseDocument db = new ODatabaseDocumentTx(DATABASE_NAME);
        if (!db.exists()) db.create();
        if (db.isClosed()) db.open("admin", "admin");
        return db;
    }

    private OClass getOClass(String className, OSchema schema,  String fieldName, OType fieldType) {
        OClass oClass;
        if (!schema.existsClass(className)) {
            oClass = schema.createClass(className);
            oClass.createProperty(fieldName, fieldType);
        } else {
            oClass = schema.getClass(className);
            if (!containsProperty(fieldType, fieldName, oClass.properties())) {
                oClass.createProperty(fieldName, fieldType);
            }
        }
        return oClass;
    }

    private boolean containsProperty(OType propertyType, String propertyName, Collection<OProperty> properties) {
        for (OProperty property : properties) {
            if (property.getName().equals(propertyName) && property.getType() == propertyType) {
                return true;
            }
        }
        return false;
    }
}
