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

package com.etendoerp.db.extended.modulescript;

/**
 * Exception thrown when table configuration processing fails during partition constraint handling.
 *
 * <p>This exception wraps underlying errors that occur during the processing of
 * partitioned table constraints, including backup operations, SQL execution, and
 * constraint recreation.
 *
 * @author Futit Services S.L.
 */
public class TableProcessingException extends Exception {

  /**
   * Constructs a new TableProcessingException with the specified detail message.
   *
   * @param message
   *     the detail message
   */
  public TableProcessingException(String message) {
    super(message);
  }

  /**
   * Constructs a new TableProcessingException with the specified detail message and cause.
   *
   * @param message
   *     the detail message
   * @param cause
   *     the cause of the exception
   */
  public TableProcessingException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new TableProcessingException wrapping the specified cause.
   *
   * @param cause
   *     the cause of the exception
   */
  public TableProcessingException(Throwable cause) {
    super(cause);
  }
}
