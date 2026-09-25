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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;
import org.openbravo.database.ConnectionProvider;

import com.etendoerp.db.extended.data.VectorEmbedProvider;
import com.etendoerp.db.extended.data.VectorSource;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Candidate;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Verdict;

/**
 * Covers the reading the window does, against the one the update does.
 *
 * <p>They are two queries over the same question, and the cost of them disagreeing is silent: the
 * window calls a source ready, the update leaves it alone, and nothing is ever indexed — or worse,
 * the update instruments a source the window never blessed and the outbox fills with events that
 * cannot be delivered.</p>
 */
class VectorSourceReadinessTest {

  @Test
  void readsADeactivatedSourceAsUnusableHoweverEnabledItSaysItIs() throws Exception {
    Candidate candidate = candidate(inactiveSource());

    assertEquals(Verdict.DISABLED, VectorSourceReadiness.verdict(candidate, null),
        "the trigger generation requires isactive as well as isenabled, so a window that looked "
            + "only at the second would promise indexing the update never installs");
  }

  @Test
  void readsAnInactiveProviderAsNoProviderAtAll() throws Exception {
    Candidate candidate = candidate(sourceWithInactiveProvider());

    assertEquals(Verdict.WITHOUT_PROVIDER, VectorSourceReadiness.verdict(candidate, null),
        "delivery resolves the provider with isactive = 'Y', so a source pointing at an inactive "
            + "one cannot be delivered and must not be called ready");
  }

  @Test
  void readsATableWithNoActiveKeyColumnAsUnindexable() throws Exception {
    Candidate candidate = candidate(readySource(), false);

    assertEquals(Verdict.WITHOUT_KEY, VectorSourceReadiness.verdict(candidate, null),
        "the generated trigger writes the record's key into the queue, so there is nothing to "
            + "instrument and the update leaves the source alone");
  }

  @Test
  void readsAFullyConfiguredSourceAsWaitingOnlyForItsCollection() throws Exception {
    Candidate candidate = candidate(readySource());

    assertEquals(Verdict.COLLECTION_MISSING, VectorSourceReadiness.verdict(candidate, null));
    assertEquals(1536, candidate.dimensions.intValue());
  }

  // --- fixtures ---------------------------------------------------------------------------------

  private static VectorSource readySource() {
    return source(true, true, provider(true));
  }

  private static VectorSource inactiveSource() {
    return source(false, true, provider(true));
  }

  private static VectorSource sourceWithInactiveProvider() {
    return source(true, true, provider(false));
  }

  private static VectorEmbedProvider provider(boolean active) {
    VectorEmbedProvider provider = mock(VectorEmbedProvider.class);
    when(provider.isActive()).thenReturn(active);
    when(provider.getDimensions()).thenReturn(1536L);
    return provider;
  }

  private static VectorSource source(boolean active, boolean enabled, VectorEmbedProvider provider) {
    VectorSource source = mock(VectorSource.class);
    when(source.getId()).thenReturn("SRC1");
    when(source.getName()).thenReturn("Example");
    when(source.getNamespace()).thenReturn("go.example");
    when(source.getDistanceMetric()).thenReturn("COSINE");
    when(source.isActive()).thenReturn(active);
    when(source.isEnabled()).thenReturn(enabled);
    when(source.getEtarcVectorEmbedProvider()).thenReturn(provider);
    return source;
  }

  /** Runs the real reading, with the column counts answered by a mock. */
  private static Candidate candidate(VectorSource source) throws Exception {
    return candidate(source, true);
  }

  private static Candidate candidate(VectorSource source, boolean hasKey) throws Exception {
    ConnectionProvider cp = mock(ConnectionProvider.class);
    when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      ResultSet rows = mock(ResultSet.class);
      when(rows.next()).thenReturn(true);
      when(rows.getInt(1)).thenReturn(sql.contains("k.iskey") ? (hasKey ? 1 : 0) : 3);
      when(rows.getInt(2)).thenReturn(2);
      PreparedStatement statement = mock(PreparedStatement.class);
      when(statement.executeQuery()).thenReturn(rows);
      return statement;
    });
    return VectorSourceReadiness.candidate(cp, source);
  }
}
