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

/** Controlled exception for vector capability and validation failures. */
public class VectorException extends RuntimeException {
  private final VectorErrorCode code;
  public VectorException(VectorErrorCode code, String message) { super(message); this.code = code; }
  public VectorException(VectorErrorCode code, String message, Throwable cause) { super(message, cause); this.code = code; }
  public VectorErrorCode getCode() { return code; }
}
