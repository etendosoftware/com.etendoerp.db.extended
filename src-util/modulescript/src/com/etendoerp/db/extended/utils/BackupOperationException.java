/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing rights
 * and limitations under the License.
 * All portions are Copyright © 2021–2025 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */

package com.etendoerp.db.extended.utils;

/**
 * Exception thrown when a backup operation or SQL execution with backup fails.
 *
 * <p>This exception provides detailed information about backup availability
 * when SQL execution fails after a backup has been created, allowing manual
 * restoration if needed.
 *
 * @author Futit Services S.L.
 */
public class BackupOperationException extends Exception {

  /**
   * Constructs a new BackupOperationException with the specified detail message.
   *
   * @param message
   *     the detail message
   */
  public BackupOperationException(String message) {
    super(message);
  }

  /**
   * Constructs a new BackupOperationException with the specified detail message and cause.
   *
   * @param message
   *     the detail message
   * @param cause
   *     the cause of the exception
   */
  public BackupOperationException(String message, Throwable cause) {
    super(message, cause);
  }
}
