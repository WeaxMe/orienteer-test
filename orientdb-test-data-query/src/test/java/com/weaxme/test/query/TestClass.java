package com.weaxme.test.query;


import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClass {

    private static final Logger LOG = LoggerFactory.getLogger(TestClass.class);

    private TestLinkedClass testLinkedClass;

    @Before
    public void init() {
        testLinkedClass = new TestLinkedClass();
    }

    @Test
    public void testLink() {
        testLinkedClass.create();
        LOG.info("link property: {}", testLinkedClass.getPropertyLink());
        testLinkedClass.deleteLink();
        LOG.info("link property: {}", testLinkedClass.getPropertyLink());
    }
}
