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

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;

/**
 * Decides whether a failed delivery is worth another attempt, and when.
 *
 * <p>Only failures that can pass on their own are retried:</p>
 * <ul>
 * <li>HTTP 408, 429 and any 5xx: the provider timed out, is throttling or is briefly
 * unavailable;</li>
 * <li>a timeout or any other I/O failure with no HTTP answer: the network, not the request.</li>
 * </ul>
 *
 * <p>Everything else is permanent: any other 4xx means the request itself is wrong and sending it
 * again gets the same answer, a malformed endpoint URL is an I/O failure that no retry repairs,
 * and a failure that is not a provider call at all (a missing source, a database error) is a
 * configuration problem. That includes 401 and 403 on purpose: a rejected API
 * key does not fix itself, so retrying only spends provider calls. Once the key is corrected the
 * events are put back with a requeue, scoped to the affected client or source if needed.</p>
 *
 * <p>The decision reads the status the provider carried in {@link VectorProviderException} and the
 * exception types in the cause chain, never the wording of a message.</p>
 */
final class VectorDeliveryRetryPolicy {
  /** Wait before the first retry; it doubles on every further attempt. */
  static final Duration BASE_BACKOFF = Duration.ofSeconds(60);
  /** The exponential backoff never grows past this. */
  static final Duration MAX_BACKOFF = Duration.ofHours(1);
  /** A Retry-After is honoured up to this, so a malformed or hostile value cannot park an event. */
  static final Duration MAX_RETRY_AFTER = Duration.ofHours(24);

  /** How deep the cause chain is walked, so a cyclic or absurdly nested chain cannot loop. */
  private static final int MAX_CAUSE_DEPTH = 20;

  private VectorDeliveryRetryPolicy() {
  }

  /** Whether the failure can pass on its own, so the event deserves another attempt. */
  static boolean isTransient(Throwable failure) {
    int depth = 0;
    for (Throwable cause = failure; cause != null && depth < MAX_CAUSE_DEPTH;
        cause = next(cause), depth++) {
      if (cause instanceof VectorProviderException) {
        int status = ((VectorProviderException) cause).getHttpStatus();
        return status == 408 || status == 429 || status >= 500;
      }
      if (cause instanceof MalformedURLException) {
        return false;
      }
      if (cause instanceof IOException) {
        return true;
      }
    }
    return false;
  }

  /**
   * How long to wait before the next attempt.
   *
   * @param attempt
   *     the attempt that just failed, starting at one
   * @param failure
   *     what it failed with, read for the provider's Retry-After
   * @return the larger of the exponential backoff and the Retry-After, bounded
   */
  static Duration delay(int attempt, Throwable failure) {
    int doublings = Math.max(0, Math.min(attempt - 1, 30));
    Duration backoff = BASE_BACKOFF.multipliedBy(1L << doublings);
    if (backoff.compareTo(MAX_BACKOFF) > 0) {
      backoff = MAX_BACKOFF;
    }
    Duration retryAfter = retryAfter(failure);
    if (retryAfter == null || retryAfter.compareTo(backoff) <= 0) {
      return backoff;
    }
    return retryAfter.compareTo(MAX_RETRY_AFTER) > 0 ? MAX_RETRY_AFTER : retryAfter;
  }

  private static Duration retryAfter(Throwable failure) {
    int depth = 0;
    for (Throwable cause = failure; cause != null && depth < MAX_CAUSE_DEPTH;
        cause = next(cause), depth++) {
      if (cause instanceof VectorProviderException) {
        return ((VectorProviderException) cause).getRetryAfter();
      }
    }
    return null;
  }

  private static Throwable next(Throwable cause) {
    return cause.getCause() == cause ? null : cause.getCause();
  }
}
