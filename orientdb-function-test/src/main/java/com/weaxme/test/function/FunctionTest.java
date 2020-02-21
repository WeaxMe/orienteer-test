package com.weaxme.test.function;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class FunctionTest {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionTest.class);

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
            "var res = db.command(\"update OUser set firstName='Test' where name = ?\", name);" +
            "print(res);" +
            "print(res.next());" +
            "");
    testFunc.setParameters(Collections.singletonList("name"));
    testFunc.save();

    return testFunc;
  }


  public static void main(String[] args) throws InterruptedException {
    OrientDB orientDB = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(DB_NAME, ODatabaseType.MEMORY);
    try {
      ODatabaseSession db = orientDB.cachedPool(DB_NAME, "admin", "admin").acquire();
      initSchema(db);
      OFunction function = initFunction(db);

      LOG.info("Start execute function: {}", function.getName());
      function.execute("admin");
      LOG.info("End execute function: {}", function.getName());

    } finally {
      orientDB.close();
    }
  }

}
