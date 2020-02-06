package com.weaxme.test.restricted;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class OrientDbRestrictedTest {

  private static final Logger LOG = LoggerFactory.getLogger(OrientDbRestrictedTest.class);

  private static final String DB_NAME = "test";
  private static final String TESTER_USERNAME = "tester";
  private static final String TESTER_PASSWORD = "tester";

  private static void initData(ODatabaseSession db) {
    OSchema schema = db.getMetadata().getSchema();

    OClass userClass = schema.getClass("OUser");
    userClass.addSuperClass(schema.getClass("ORestricted"));

    OClass roleClass = schema.getClass("ORole");
    roleClass.addSuperClass(schema.getClass("ORestricted"));

    ORole testerRole = db.getMetadata().getSecurity().createRole("tester-role", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);
    testerRole.grant(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.CLASS, ORole.CLASS_NAME, ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_NONE);
    testerRole.grant(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.DATABASE, "cluster", ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.CLUSTER, "internal", ORole.PERMISSION_READ);
    testerRole.grant(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);


    testerRole.getDocument().field(ORestrictedOperation.ALLOW_READ.getFieldName(), Collections.singletonList(testerRole.getDocument()));

    testerRole.save();

    OElement tester = db.newElement("OUser");
    tester.setProperty("name", TESTER_USERNAME);
    tester.setProperty("password", TESTER_PASSWORD);
    tester.setProperty("status", OSecurityUser.STATUSES.ACTIVE.name());
    tester.setProperty("_allowRead", Collections.singleton(tester));
    tester.setProperty("roles", Collections.singleton(testerRole.getDocument()));
    tester.save();
  }

  private static OElement getTesterElement(ODatabaseSession db) {
    OResultSet result = db.query("select from OUser where name = ?", TESTER_USERNAME);
    return result.hasNext() ? result.next().getElement().orElse(null) : null;
  }

  public static void main(String[] args) {
    OrientDB orientDB = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(DB_NAME, ODatabaseType.MEMORY);
    try {
      ODatabaseSession adminDb = orientDB.cachedPool(DB_NAME, "admin", "admin").acquire();
      initData(adminDb);

      LOG.info("Tester from admin database: {}", getTesterElement(adminDb));

      adminDb.close();

      ODatabaseSession testerDb = orientDB.cachedPool(DB_NAME, TESTER_USERNAME, TESTER_PASSWORD).acquire();

      LOG.info("Tester from tester database: {}", getTesterElement(testerDb));

      testerDb.close();

    } finally {
      orientDB.close();
    }
  }

}
