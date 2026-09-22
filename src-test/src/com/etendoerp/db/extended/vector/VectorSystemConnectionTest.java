/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;

import org.junit.jupiter.api.Test;

/**
 * Covers when the system connection declines to exist.
 *
 * <p>Returning null has to mean "this installation records no system credentials", because the
 * caller reads it as permission to fall back to the application user. Throwing instead, or
 * returning a connection built from half a configuration, would turn a normal installation into a
 * failed provisioning.</p>
 */
class VectorSystemConnectionTest {

  @Test
  void hasNoConnectionWhenThereAreNoProperties() throws Exception {
    assertNull(VectorSystemConnection.open(null));
  }

  @Test
  void hasNoConnectionWhenAnyCredentialIsMissing() throws Exception {
    assertNull(VectorSystemConnection.open(properties(null, "secret", "jdbc:x", "db")),
        "no system user");
    assertNull(VectorSystemConnection.open(properties("postgres", null, "jdbc:x", "db")),
        "no password: an installation that blanked it out has none to use");
    assertNull(VectorSystemConnection.open(properties("postgres", "secret", null, "db")),
        "no url");
    assertNull(VectorSystemConnection.open(properties("postgres", "secret", "jdbc:x", null)),
        "no database name");
  }

  @Test
  void treatsBlankAsMissing() throws Exception {
    assertNull(VectorSystemConnection.open(properties("postgres", "   ", "jdbc:x", "db")),
        "a property left empty is not a credential, and building a URL from it would fail far "
            + "from the place that could explain why");
  }

  private static Properties properties(String user, String password, String url, String sid) {
    Properties properties = new Properties();
    if (user != null) {
      properties.setProperty("bbdd.systemUser", user);
    }
    if (password != null) {
      properties.setProperty("bbdd.systemPassword", password);
    }
    if (url != null) {
      properties.setProperty("bbdd.url", url);
    }
    if (sid != null) {
      properties.setProperty("bbdd.sid", sid);
    }
    return properties;
  }
}
