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
 * Base exception for constraint processing operations in partitioned tables.
 *
 * <p>This exception serves as the parent class for all constraint-related exceptions
 * that can occur during the processing of database constraints for partitioned tables.
 * It provides a consistent error handling mechanism across the constraint processing
 * workflow.
 *
 * @author Futit Services S.L.
 * @since ETP-2450
 */
public class ConstraintProcessingException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new ConstraintProcessingException with the specified detail message.
   *
   * @param message
   *     the detail message explaining the cause of the exception
   */
  public ConstraintProcessingException(String message) {
    super(message);
  }

  /**
   * Constructs a new ConstraintProcessingException with the specified detail message
   * and cause.
   *
   * @param message
   *     the detail message explaining the cause of the exception
   * @param cause
   *     the underlying cause of the exception
   */
  public ConstraintProcessingException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new ConstraintProcessingException with the specified cause.
   *
   * @param cause
   *     the underlying cause of the exception
   */
  public ConstraintProcessingException(Throwable cause) {
    super(cause);
  }

  /**
   * Exception thrown when a table's XML definition file cannot be found.
   *
   * <p>This typically occurs when attempting to process constraints for a table
   * that doesn't have a corresponding XML definition in the expected location.
   */
  public static class TableXmlNotFoundException extends ConstraintProcessingException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new TableXmlNotFoundException for the specified table.
     *
     * @param tableName
     *     the name of the table whose XML file was not found
     */
    public TableXmlNotFoundException(String tableName) {
      super("Entity XML file for " + tableName + " not found.");
    }
  }

  /**
   * Exception thrown when a primary key definition cannot be found in a table's XML.
   *
   * <p>This occurs when the XML definition exists but doesn't contain a valid
   * primary key definition, which is required for constraint processing.
   */
  public static class PrimaryKeyNotFoundException extends ConstraintProcessingException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new PrimaryKeyNotFoundException for the specified table.
     *
     * @param tableName
     *     the name of the table whose primary key was not found
     */
    public PrimaryKeyNotFoundException(String tableName) {
      super("Primary Key for entity " + tableName + " not found in XML.");
    }
  }

  /**
   * Exception thrown when a database constraint operation fails.
   *
   * <p>This exception wraps underlying database-related exceptions that occur
   * during constraint existence checks or other database operations.
   */
  public static class DatabaseConstraintException extends ConstraintProcessingException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new DatabaseConstraintException with the underlying cause.
     *
     * @param cause
     *     the underlying database exception
     */
    public DatabaseConstraintException(Throwable cause) {
      super("Database constraint operation failed", cause);
    }

    /**
     * Constructs a new DatabaseConstraintException with a specific message and cause.
     *
     * @param message
     *     the detail message
     * @param cause
     *     the underlying database exception
     */
    public DatabaseConstraintException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
