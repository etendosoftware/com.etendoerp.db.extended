package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;
import org.openbravo.database.ConnectionProvider;

/**
 * Unit tests for the read-only pgvector capability inspection.
 */
class VectorCapabilityServiceTest {

  @Test
  void reportsUnavailableWhenTheExtensionIsNotOfferedByTheServer() throws Exception {
    VectorCapability capability = inspect(false, false);

    assertEquals(VectorCapabilityState.UNAVAILABLE, capability.getState());
  }

  @Test
  void reportsAvailableWhenTheExtensionIsNotActiveInTheDatabase() throws Exception {
    VectorCapability capability = inspect(true, false);

    assertEquals(VectorCapabilityState.AVAILABLE, capability.getState());
  }

  @Test
  void reportsActiveWhenTheExtensionIsInstalledInTheDatabase() throws Exception {
    VectorCapability capability = inspect(true, true);

    assertEquals(VectorCapabilityState.ACTIVE, capability.getState());
  }

  @Test
  void reportsFailedWhenCatalogInspectionCannotBeCompleted() throws Exception {
    ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
    when(connectionProvider.getPreparedStatement(anyString())).thenThrow(new RuntimeException("database down"));

    VectorCapability capability = new VectorCapabilityService(connectionProvider).inspect();

    assertEquals(VectorCapabilityState.FAILED, capability.getState());
    assertFalse(capability.getDiagnostic().contains("database down"));
  }

  private VectorCapability inspect(boolean available, boolean installed) throws Exception {
    ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(connectionProvider.getPreparedStatement(VectorCapabilityService.CAPABILITY_SQL)).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean("available")).thenReturn(available);
    when(resultSet.getBoolean("installed")).thenReturn(installed);

    VectorCapability capability = new VectorCapabilityService(connectionProvider).inspect();

    verify(statement).setString(1, "vector");
    verify(statement).setString(2, "vector");
    assertFalse(VectorCapabilityService.CAPABILITY_SQL.toUpperCase().contains("CREATE EXTENSION"));
    assertTrue(VectorCapabilityService.CAPABILITY_SQL.contains("pg_available_extensions"));
    assertTrue(VectorCapabilityService.CAPABILITY_SQL.contains("pg_extension"));
    return capability;
  }
}
