package com.weaxme.test.query;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertTrue;

public class TestPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(TestPreparedStatement.class);

    private final String className       = "TestClass";
    private final String dateField       = "date";
    private final String dateTimeField   = "dateTime";
    private final String dateFormat      = "yyyy-MM-dd";
    private final String dateTimeFormat  = "yyyy-MM-dd HH:mm:ss";
    private final String dbUrl           = "plocal:/tmp/testdb";
    private final String dateValue       = "2017-07-17";
    private final String dateTimeValue   = "2017-07-17 23:09:00";

    private ODatabaseDocument db;

    @Before
    public void initDatabase() {
        db = new ODatabaseDocumentTx(dbUrl);
        if (!db.exists()) {
            db.create();
        }
        openDatabase();
        createClass();
        listDocs();
    }

    @After
    public void dropDatabase() {
        openDatabase();
        db.drop();
    }

    private void createClass() {
        OClass testClass = db.getMetadata().getSchema().createClass(className);
        testClass.createProperty(dateField, OType.DATE);
        testClass.createProperty(dateTimeField, OType.DATETIME);
        ODocument document = new ODocument(testClass.getName());

        try {
            document.field(dateField, new SimpleDateFormat(dateFormat).parse(dateValue));
            document.field(dateTimeField, new SimpleDateFormat(dateTimeFormat).parse(dateTimeValue));
            document.save();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        db.commit();
    }

    @Test
    public void testDateStringPreparedStatement() {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s = :%s", className, dateField, dateField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        params.put(dateField, dateValue);
        assertTrue(db.query(query, params).size() == 1);
    }

    @Test
    public void testDateJavaClassPreparedStatement() throws ParseException {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s = :%s", className, dateField, dateField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        params.put(dateField, new SimpleDateFormat(dateFormat).parse(dateValue));
        assertTrue(db.query(query, params).size() == 1);
    }

    @Test
    public void testDateTimeStringPreparedStatement() {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s = :%s", className, dateTimeField, dateTimeField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        params.put(dateTimeField, dateTimeValue);
        assertTrue(db.query(query, params).size() == 1);
    }

    @Test
    public void testDateTimeJavaClassPreparedStatement() throws ParseException {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s = :%s", className, dateTimeField, dateTimeField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        params.put(dateTimeField, new SimpleDateFormat(dateTimeFormat).parse(dateTimeValue));
        assertTrue(db.query(query, params).size() == 1);
    }

    @Test
    public void testDateTimeCollectionPreparedStatement() throws ParseException {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s IN :%s", className, dateTimeField, dateTimeField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        Date date = new SimpleDateFormat(dateTimeFormat).parse(dateTimeValue);
        params.put(dateTimeField, Arrays.asList(date));
        assertTrue(db.query(query, params).size() == 1);
    }

    @Test
    public void testDateCollectionPreparedStatement() throws ParseException {
        openDatabase();
        OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<>(
                String.format("SELECT FROM %s WHERE %s IN :%s", className, dateField, dateField));
        LOG.info("query: {}", query);
        Map<String, Object> params = new HashMap<>();
        Date date = new SimpleDateFormat(dateFormat).parse(dateValue);
        params.put(dateField, Arrays.asList(date));
        assertTrue(db.query(query, params).size() == 1);
    }

    private void listDocs() {
        List<ODocument> docs = db.query(new OSQLSynchQuery<List<ODocument>>("SELECT FROM " + className));
        LOG.info("Documents of {} size: {}", className, docs.size());
        for (ODocument document : docs) {
            LOG.info("document: {}", document);
        }
    }

    private void openDatabase() {
        if (db.isClosed()) {
            db.open("admin", "admin");
        }
    }
}
