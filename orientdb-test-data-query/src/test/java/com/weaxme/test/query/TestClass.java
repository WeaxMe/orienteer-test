package com.weaxme.test.query;


import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestClass {

    private static final Logger LOG = LoggerFactory.getLogger(TestClass.class);

    private static final String QUERY_TEMPLATE = "select from %s where %s='%s'";

    private TestQueryManager manager;

    @Before
    public void init() {
        manager = new TestQueryManager();
    }

    @After
    public void clear() {
        manager.clear();
    }


    @Test
    public void testDateTime() {
        String datetime = "2017-05-09 01:45:30";
        String field = "datetime";
        String className = "TestDateTimeClass";
        String sql = String.format(QUERY_TEMPLATE, className, field, datetime);
        ODocument doc = manager.createDocumentField(className, field, OType.DATETIME, datetime);
        LOG.info("Created document: {} ", doc);
        manager.showOClassDocuments(className);
        List<ODocument> documents = manager.executeQuery(sql);
        assertTrue("Result of query is empty!", documents.size() > 0);
        printQueryResult(documents);
    }

    @Test
    public void testInteger() {
        String integer = "27";
        String field = "integer";
        String className = "TestIntegerClass";
        String sql = String.format(QUERY_TEMPLATE, className, field, integer);
        ODocument doc = manager.createDocumentField(className, field, OType.INTEGER, integer);
        LOG.info("Created document: {} ", doc);
        manager.showOClassDocuments(className);
        List<ODocument> documents = manager.executeQuery(sql);
        manager.showOClassDocuments(className);
        assertTrue("Result of query is empty!", documents.size() > 0);
        printQueryResult(documents);
    }

    @Test
    public void testQueryDate() {
        String date = "2017-05-09";
        String field = "date";
        String className = "TestDateClass";
        String sql = String.format("select from %s where %s like '%s'", className, field, date);
        ODocument doc = manager.createDocumentField(className, field, OType.DATE, date);
        LOG.info("Created document: {} ", doc);
        manager.showOClassDocuments(className);
        List<ODocument> documents = manager.executeQuery(sql);
        assertTrue("Result of query is empty!", documents.size() > 0);
        printQueryResult(documents);
    }


    private void printQueryResult(List<ODocument> documents) {
        LOG.info("Result of query:");
        int counter = 0;
        for (ODocument document : documents) {
            LOG.info("{}. document: {}", counter++, document);
        }
    }
}
