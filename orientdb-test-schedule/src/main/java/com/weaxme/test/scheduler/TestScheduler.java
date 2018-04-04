package com.weaxme.test.scheduler;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.schedule.OScheduledEventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TestScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestScheduler.class);

    public void run() throws InterruptedException {
        long timeout = 60_000;
        String cronExpr = "0 0/1 * 1/1 * ? *";
        ODatabaseDocument db = createDatabase();
        OScheduledEvent e = createEvent(cronExpr, timeout);
        db.getMetadata().getScheduler().scheduleEvent(e);

        Thread.currentThread().join(120_000);
    }

    private OScheduledEvent createEvent(String rule, long timeout) {
        Date startTime = new Date(System.currentTimeMillis() + timeout);
        LOG.info("Scheduler start time: {}", startTime);
        return new OScheduledEventBuilder()
                .setName("test")
                .setFunction(createFunction())
                .setRule(rule)
                .setStartTime(startTime)
                .build();
    }

    private OFunction createFunction() {
        OFunction func = new OFunction();
        func.setName("testFunction");
        func.setLanguage("javascript");
        func.setCode("print('Test scheduler');");
        func.save();
        return func;

    }

    private ODatabaseDocument createDatabase() {
        ODatabaseDocument db = new ODatabaseDocumentTx("memory:test");
        db.create();
        if (db.isClosed()) db.open("admin", "admin");
        return db;
    }

    public static void main(String [] args) throws InterruptedException {
        new TestScheduler().run();
    }
}
