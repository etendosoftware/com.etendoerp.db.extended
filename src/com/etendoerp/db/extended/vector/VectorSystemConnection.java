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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A connection as the database's system user, for the one statement the application user cannot
 * issue.
 *
 * <p>Creating a PostgreSQL extension requires a privilege the application role normally does not
 * have, and normally should not have. Leaving it to a DBA works, but doing it outside an update
 * leaves the structure checksum reporting local changes that the next update.database then refuses
 * to run over -- so the extension gets created here instead, during the update, where the same run
 * re-stamps the checksum afterwards and nothing is left to accept.</p>
 *
 * <p>These credentials are the ones {@code update.database} already runs with: {@code build.xml}
 * hands {@code bbdd.systemUser} and {@code bbdd.systemPassword} to the DBSM task on every update.
 * They are read from the properties file the module script was given, used for a single statement
 * on a connection of its own, and closed. An installation that blanked them out simply has none,
 * and provisioning falls back to trying as the application user.</p>
 */
final class VectorSystemConnection {

  private VectorSystemConnection() {
  }

  /**
   * Opens a connection as the system user.
   *
   * @param properties
   *     the Openbravo properties the caller was configured with
   * @return the connection, or {@code null} when the installation records no system credentials
   * @throws SQLException
   *     if the credentials are there but the connection cannot be opened
   */
  static Connection open(Properties properties) throws SQLException {
    if (properties == null) {
      return null;
    }
    String user = properties.getProperty("bbdd.systemUser");
    String password = properties.getProperty("bbdd.systemPassword");
    String url = properties.getProperty("bbdd.url");
    String sid = properties.getProperty("bbdd.sid");
    if (isBlank(user) || isBlank(password) || isBlank(url) || isBlank(sid)) {
      return null;
    }
    // bbdd.url stops at the server; the database is bbdd.sid, which is how every other consumer of
    // these two properties assembles the owner URL.
    return DriverManager.getConnection(url + "/" + sid, user, password);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
