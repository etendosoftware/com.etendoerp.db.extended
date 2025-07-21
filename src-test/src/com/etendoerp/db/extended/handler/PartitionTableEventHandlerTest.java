package com.etendoerp.db.extended.handler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PartitionTableEventHandlerTest {

  private PartitionTableEventHandler handler;

  private MockedStatic<ModelProvider> mockedModelProvider;
  private MockedStatic<OBMessageUtils> mockedMessageUtils;
  private MockedStatic<OBDal> mockedOBDal;

  @Mock private ModelProvider modelProvider;
  @Mock private Entity tableConfigEntity;
  @Mock private EntityNewEvent newEvent;
  @Mock private EntityDeleteEvent deleteEvent;
  @Mock private TableConfig tableConfig;
  @Mock private Table table;
  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;

  @BeforeEach
  public void setUp() throws Exception {
    mockedModelProvider = mockStatic(ModelProvider.class);
    mockedMessageUtils = mockStatic(OBMessageUtils.class);
    mockedOBDal = mockStatic(OBDal.class);

    mockedModelProvider.when(ModelProvider::getInstance).thenReturn(modelProvider);
    when(modelProvider.getEntity(TableConfig.ENTITY_NAME)).thenReturn(tableConfigEntity);

    handler = new TestablePartitionTableEventHandler(new Entity[]{ tableConfigEntity });

    when(newEvent.getTargetInstance()).thenReturn(tableConfig);
    when(deleteEvent.getTargetInstance()).thenReturn(tableConfig);
    when(tableConfig.getTable()).thenReturn(table);
    when(table.getDBTableName()).thenReturn("mock_table");
    when(tableConfig.getEntity()).thenReturn(tableConfigEntity);
  }

  @AfterEach
  public void tearDown() {
    mockedModelProvider.close();
    mockedMessageUtils.close();
    mockedOBDal.close();
  }

  @Test
  public void testOnNew_WithUniqueConstraint_ThrowsException() throws Exception {
    setupMockJdbc("Y");
    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_TableHasUnique"))
        .thenReturn("Table has unique constraint");

    OBException ex = assertThrows(OBException.class, () -> handler.onNew(newEvent));
    assertEquals("Table has unique constraint", ex.getMessage());
  }

  @Test
  public void testOnNew_WithoutUniqueConstraint_DoesNotThrow() throws Exception {
    setupMockJdbc("N");
    assertDoesNotThrow(() -> handler.onNew(newEvent));
  }

  @Test
  public void testOnNew_WithSQLException_ThrowsOBException() throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);

    when(obDal.getConnection(false)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenThrow(new SQLException("SQL error"));

    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_CouldNotRetrieveTables"))
        .thenReturn("Could not retrieve");

    OBException ex = assertThrows(OBException.class, () -> handler.onNew(newEvent));
    assertTrue(ex.getMessage().contains("Could not retrieve"));
  }

  @Test
  public void testOnDelete_WithPartitionedTable_ThrowsException() throws Exception {
    setupMockJdbcDelete("Y");
    mockedMessageUtils.when(() -> OBMessageUtils.messageBD("ETARC_DeleteAlreadyPartTable"))
        .thenReturn("Already partitioned");

    OBException ex = assertThrows(OBException.class, () -> handler.onDelete(deleteEvent));
    assertEquals("Already partitioned", ex.getMessage());
  }

  @Test
  public void testOnDelete_WithNonPartitionedTable_DoesNotThrow() throws Exception {
    setupMockJdbcDelete("N");
    assertDoesNotThrow(() -> handler.onDelete(deleteEvent));
  }

  private void setupMockJdbc(String hasUniqueValue) throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);
    when(obDal.getConnection(false)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("hasunique")).thenReturn(hasUniqueValue);
  }

  private void setupMockJdbcDelete(String isPartitionedValue) throws Exception {
    OBDal obDal = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(obDal);
    when(obDal.getConnection(true)).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("ispartitioned")).thenReturn(isPartitionedValue);
  }

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
