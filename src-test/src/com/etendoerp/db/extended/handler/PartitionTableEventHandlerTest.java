package com.etendoerp.db.extended.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.client.kernel.event.EntityNewEvent;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.model.ad.datamodel.Table;

import com.etendoerp.db.extended.data.TableConfig;

/**
 * Unit tests for {@link PartitionTableEventHandler}.
 * <p>
 * These tests verify the behavior of the event handler when new {@link TableConfig}
 * entities are inserted or deleted, specifically ensuring it correctly handles:
 * <ul>
 *   <li>Detection of unique constraints on partitioned tables</li>
 *   <li>Validation of partitioned status before deletion</li>
 *   <li>Expected exceptions in case of configuration or database issues</li>
 * </ul>
 * <p>
 * The test class uses Mockito for mocking static classes like {@link OBDal},
 * {@link OBMessageUtils}, and {@link ModelProvider}, and also mocks JDBC connections.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PartitionTableEventHandlerTest {

  private PartitionTableEventHandler handler;

  private MockedStatic<ModelProvider> mockedModelProvider;
  private MockedStatic<OBMessageUtils> mockedMessageUtils;
  private MockedStatic<OBDal> mockedOBDal;

  @Mock
  private ModelProvider modelProvider;
  @Mock
  private Entity tableConfigEntity;
  @Mock
  private EntityNewEvent newEvent;
  @Mock
  private EntityDeleteEvent deleteEvent;
  @Mock
  private TableConfig tableConfig;
  @Mock
  private Table table;
  @Mock
  private Connection connection;
  @Mock
  private PreparedStatement preparedStatement;
  @Mock
  private ResultSet resultSet;

  /**
   * Sets up mock behavior for static Openbravo APIs and entity metadata.
   * <p>
   * This includes mocking {@link ModelProvider}, {@link OBMessageUtils}, and {@link OBDal},
   * as well as configuring basic return values from mocked {@link TableConfig} and {@link Table} objects.
   */
  @BeforeEach
  void setUp() throws Exception {
    mockedModelProvider = mockStatic(ModelProvider.class);
    mockedMessageUtils = mockStatic(OBMessageUtils.class);
    mockedOBDal = mockStatic(OBDal.class);

    mockedModelProvider.when(ModelProvider::getInstance).thenReturn(modelProvider);
    when(modelProvider.getEntity(TableConfig.ENTITY_NAME)).thenReturn(tableConfigEntity);

    handler = new TestablePartitionTableEventHandler(new Entity[]{tableConfigEntity});

    when(newEvent.getTargetInstance()).thenReturn(tableConfig);
    when(deleteEvent.getTargetInstance()).thenReturn(tableConfig);
    when(tableConfig.getTable()).thenReturn(table);
    when(table.getDBTableName()).thenReturn("mock_table");
    when(tableConfig.getEntity()).thenReturn(tableConfigEntity);
  }

  /**
   * Releases mocked static classes after each test to avoid leaks or cross-test interference.
   */
  @AfterEach
  void tearDown() {
    mockedModelProvider.close();
    mockedMessageUtils.close();
    mockedOBDal.close();
  }

  /**
   * Verifies that inserting a {@link TableConfig} for a table that has a unique constraint
   * throws an {@link OBException} with the appropriate error message.
   */
  @Test
  void testOnNew_WithUniqueConstraint_ThrowsException() throws Exception {
    setupMockJdbc("Y");
    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_TableHasUnique"))
        .thenReturn("Table has unique constraint");

    OBException ex = assertThrows(OBException.class, () -> handler.onNew(newEvent));
    assertEquals("Table has unique constraint", ex.getMessage());
  }

  /**
   * Verifies that inserting a {@link TableConfig} for a table without a unique constraint
   * does not throw any exceptions.
   */
  @Test
  void testOnNew_WithoutUniqueConstraint_DoesNotThrow() throws Exception {
    setupMockJdbc("N");
    assertDoesNotThrow(() -> handler.onNew(newEvent));
  }

  /**
   * Verifies that if a {@link SQLException} occurs during unique constraint check,
   * an {@link OBException} is thrown with the proper translated message.
   */
  @Test
  void testOnNew_WithSQLException_ThrowsOBException() throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);

    when(obDal.getConnection(false)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenThrow(new SQLException("SQL error"));

    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_CouldNotRetrieveTables"))
        .thenReturn("Could not retrieve");

    OBException ex = assertThrows(OBException.class, () -> handler.onNew(newEvent));
    assertTrue(ex.getMessage().contains("Could not retrieve"));
  }

  /**
   * Verifies that attempting to delete a {@link TableConfig} whose underlying table
   * is partitioned results in an {@link OBException}.
   */
  @Test
  void testOnDelete_WithPartitionedTable_ThrowsException() throws Exception {
    setupMockJdbcDelete("Y");
    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_DeleteAlreadyPartTable"))
        .thenReturn("Already partitioned");

    OBException ex = assertThrows(OBException.class, () -> handler.onDelete(deleteEvent));
    assertEquals("Already partitioned", ex.getMessage());
  }

  /**
   * Verifies that deleting a {@link TableConfig} for a non-partitioned table proceeds normally.
   */
  @Test
  void testOnDelete_WithNonPartitionedTable_DoesNotThrow() throws Exception {
    setupMockJdbcDelete("N");
    assertDoesNotThrow(() -> handler.onDelete(deleteEvent));
  }

  /**
   * Sets up mocked JDBC calls to simulate whether a table has a unique constraint.
   *
   * @param hasUniqueValue "Y" or "N" depending on the simulated constraint presence
   */
  private void setupMockJdbc(String hasUniqueValue) throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);
    when(obDal.getConnection(false)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("hasunique")).thenReturn(hasUniqueValue);
  }

  /**
   * Sets up mocked JDBC calls to simulate whether a table is partitioned.
   *
   * @param isPartitionedValue "Y" or "N" depending on the simulated partitioning status
   */
  private void setupMockJdbcDelete(String isPartitionedValue) throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);
    when(obDal.getConnection(true)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("ispartitioned")).thenReturn(isPartitionedValue);
  }

  /**
   * Custom subclass of {@link PartitionTableEventHandler} used in tests
   * to override the {@code getObservedEntities()} method and inject mocked entities.
   */
  private static class TestablePartitionTableEventHandler extends PartitionTableEventHandler {
    private final Entity[] testEntities;

    public TestablePartitionTableEventHandler(Entity[] testEntities) {
      this.testEntities = testEntities;
    }

    @Override
    protected Entity[] getObservedEntities() {
      return testEntities;
    }
  }
}
