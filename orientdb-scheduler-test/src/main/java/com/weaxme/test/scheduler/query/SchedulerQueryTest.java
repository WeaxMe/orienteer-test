package com.weaxme.test.scheduler.query;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.schedule.OScheduledEventBuilder;
import com.orientechnologies.orient.core.schedule.OScheduler;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SchedulerQueryTest {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerQueryTest.class);

  private static final String DB_NAME = "test";

  private static void initSchema(ODatabaseSession db) {
    OClass userClass = db.getMetadata().getSchema().getClass("OUser");
    userClass.createProperty("firstName", OType.STRING);
  }

  private static OFunction initFunction(ODatabaseSession db) {
    OFunctionLibrary functionLibrary = db.getMetadata().getFunctionLibrary();

    OFunction testFunc = functionLibrary.createFunction("Test");

    testFunc.setLanguage("JavaScript");
    testFunc.setCode("" +
            "print(\"Start execute\");" +
            "var res = db.command(\"update OUser set firstName='Test' where name = ?\", name);" +
            "print(\"End execute\");" +
            "print(res.next());" +
            "");
    testFunc.setParameters(Collections.singletonList("name"));
    testFunc.save();

    return testFunc;
  }

  private static void scheduleFunction(ODatabaseSession db, OFunction function) {
    OScheduler scheduler = db.getMetadata().getScheduler();

    Map<Object, Object> args = new HashMap<>();
    args.put("name", "admin");

    OScheduledEvent event = new OScheduledEventBuilder()
            .setName("test-event")
            .setFunction(function)
            .setArguments(args)
            .setRule("0 0/1 * 1/1 * ? *")
            .setStartTime(new Date(System.currentTimeMillis() + 3000)).build();

    scheduler.scheduleEvent(event);
  }


  public static void main(String[] args) throws InterruptedException {
    OrientDB orientDB = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(DB_NAME, ODatabaseType.MEMORY);
    try {
      ODatabaseSession db = orientDB.cachedPool(DB_NAME, "admin", "admin").acquire();
      initSchema(db);
      OFunction function = initFunction(db);

      LOG.info("Schedule function");
      scheduleFunction(db, function);
      Thread.sleep(15_000);
      LOG.info("End");

    } finally {
      orientDB.close();
    }
  }

}
