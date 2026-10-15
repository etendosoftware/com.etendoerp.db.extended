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

import java.time.Duration;

/**
 * An embedding provider answered a request with an HTTP error.
 *
 * <p>The status and the {@code Retry-After} the provider sent travel as data, so whether a delivery
 * is worth retrying is decided on them and never on the wording of the message.</p>
 */
public class VectorProviderException extends VectorException {
  private final int httpStatus;
  private final transient Duration retryAfter;

  /**
   * @param httpStatus
   *     the status the provider answered with
   * @param retryAfter
   *     how long the provider asked to wait before the next request, or {@code null} when it did
   *     not say
   * @param message
   *     what failed, for the event's last error
   */
  public VectorProviderException(int httpStatus, Duration retryAfter, String message) {
    super(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, message);
    this.httpStatus = httpStatus;
    this.retryAfter = retryAfter;
  }

  public int getHttpStatus() { return httpStatus; }

  /** How long the provider asked to wait, or {@code null} when it did not say. */
  public Duration getRetryAfter() { return retryAfter; }
}
