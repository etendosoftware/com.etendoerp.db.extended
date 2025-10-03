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

package com.etendoerp.db.extended.buildvalidation;

/**
 * Exception thrown when build validation operations fail during the exclude filter generation process.
 *
 * <p>This exception is specifically designed for the build validation context where the system
 * processes table configurations, XML files, and generates exclude filters for database constraints.
 * It provides specific error handling for operations that are part of the build validation workflow.
 *
 * <h3>Common Use Cases:</h3>
 * <ul>
 *   <li>Database configuration processing failures</li>
 *   <li>XML file processing errors during constraint discovery</li>
 *   <li>Table partitioning configuration validation errors</li>
 *   <li>Exclude filter generation failures</li>
 * </ul>
 *
 * @author Futit Services S.L.
 * @since ETP-2450
 */
public class BuildValidationException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new BuildValidationException with the specified detail message.
   *
   * @param message
   *     the detail message explaining the cause of the exception
   */
  public BuildValidationException(String message) {
    super(message);
  }

  /**
   * Constructs a new BuildValidationException with the specified detail message and cause.
   *
   * @param message
   *     the detail message explaining the cause of the exception
   * @param cause
   *     the underlying cause of the exception
   */
  public BuildValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new BuildValidationException with the specified cause.
   *
   * @param cause
   *     the underlying cause of the exception
   */
  public BuildValidationException(Throwable cause) {
    super(cause);
  }
}
