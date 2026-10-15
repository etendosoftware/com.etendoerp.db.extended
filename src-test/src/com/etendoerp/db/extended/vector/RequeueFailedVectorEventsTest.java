package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.service.db.DalConnectionProvider;

/**
 * Exercises the scoped requeue of failed indexing events launched from the process window.
 *
 * <p>An empty selector reaches the handler in more than one spelling, and every one of them has to
 * mean "no scope", or a requeue meant for everyone would silently match nothing.</p>
 *
 * @covers com.etendoerp.db.extended.vector.RequeueFailedVectorEvents
 */
class RequeueFailedVectorEventsTest {

  private OBDal dal;
  private MockedStatic<OBDal> dalStatic;
  private MockedStatic<OBMessageUtils> messages;
  private MockedConstruction<DalConnectionProvider> connections;

  @BeforeEach
  void setUp() {
    dal = mock(OBDal.class);
    dalStatic = Mockito.mockStatic(OBDal.class);
    dalStatic.when(OBDal::getInstance).thenReturn(dal);
    messages = Mockito.mockStatic(OBMessageUtils.class);
    messages.when(() -> OBMessageUtils.getI18NMessage(eq("ETARC_VectorFailedRequeued"), any(String[].class)))
        .thenAnswer(call -> {
          String[] args = call.getArgument(1);
          return "requeued " + args[0] + " of at most " + args[1];
        });
    connections = Mockito.mockConstruction(DalConnectionProvider.class);
  }

  @AfterEach
  void tearDown() {
    connections.close();
    messages.close();
    dalStatic.close();
  }

  // ---------------------------------------------------------------- optionalId

  @Test
  void readsAJsonNullAsNoScope() throws Exception {
    assertNull(RequeueFailedVectorEvents.optionalId(
        new JSONObject().put("AD_Client_ID", JSONObject.NULL), "AD_Client_ID"));
  }

  @Test
  void readsTheStringNullAsNoScope() throws Exception {
    assertNull(RequeueFailedVectorEvents.optionalId(
        new JSONObject().put("AD_Client_ID", "null"), "AD_Client_ID"),
        "the client sends an emptied selector as the literal string \"null\"");
  }

  @Test
  void readsABlankValueAsNoScope() throws Exception {
    assertNull(RequeueFailedVectorEvents.optionalId(
        new JSONObject().put("AD_Client_ID", "   "), "AD_Client_ID"));
  }

  @Test
  void readsAnAbsentParameterOrMissingParamsAsNoScope() {
    assertNull(RequeueFailedVectorEvents.optionalId(new JSONObject(), "AD_Client_ID"));
    assertNull(RequeueFailedVectorEvents.optionalId(null, "AD_Client_ID"));
  }

  @Test
  void returnsARealIdTrimmed() throws Exception {
    assertEquals("ABC123", RequeueFailedVectorEvents.optionalId(
        new JSONObject().put("AD_Client_ID", " ABC123 "), "AD_Client_ID"));
  }

  // ---------------------------------------------------------------- doExecute

  @Test
  void requeuesWithinTheSelectedScopeAndReportsTheCount() throws Exception {
    try (MockedConstruction<VectorOutboxService> outboxes = Mockito.mockConstruction(
        VectorOutboxService.class,
        (outbox, context) -> when(outbox.requeueFailed(anyInt(), any(), any())).thenReturn(7))) {

      JSONObject result = new RequeueFailedVectorEvents().doExecute(Collections.emptyMap(),
          content(new JSONObject().put("AD_Client_ID", "client-1")
              .put("ETARC_Vector_Source_ID", "null")));

      VectorOutboxService outbox = outboxes.constructed().get(0);
      verify(outbox).requeueFailed(eq(ProcessVectorRequeueFailed.BATCH_SIZE), eq("client-1"), isNull());
      verify(dal).commitAndClose();
      verify(dal, never()).rollbackAndClose();
      JSONObject message = result.getJSONObject("message");
      assertEquals("success", message.getString("severity"));
      assertEquals("requeued 7 of at most " + ProcessVectorRequeueFailed.BATCH_SIZE,
          message.getString("text"));
    }
  }

  @Test
  void rollsBackAndReportsTheFailureMessage() throws Exception {
    try (MockedConstruction<VectorOutboxService> outboxes = Mockito.mockConstruction(
        VectorOutboxService.class,
        (outbox, context) -> when(outbox.requeueFailed(anyInt(), any(), any()))
            .thenThrow(new IllegalStateException("database unavailable")))) {

      JSONObject result = new RequeueFailedVectorEvents().doExecute(Collections.emptyMap(),
          content(new JSONObject().put("ETARC_Vector_Source_ID", "source-1")));

      verify(outboxes.constructed().get(0)).requeueFailed(ProcessVectorRequeueFailed.BATCH_SIZE, null, "source-1");
      verify(dal).rollbackAndClose();
      verify(dal, never()).commitAndClose();
      JSONObject message = result.getJSONObject("message");
      assertEquals("error", message.getString("severity"));
      assertEquals("database unavailable", message.getString("text"));
    }
  }

  @Test
  void reportsTheExceptionClassWhenTheFailureHasNoMessage() throws Exception {
    try (MockedConstruction<VectorOutboxService> outboxes = Mockito.mockConstruction(
        VectorOutboxService.class,
        (outbox, context) -> when(outbox.requeueFailed(anyInt(), any(), any()))
            .thenThrow(new NullPointerException()))) {

      JSONObject result = new RequeueFailedVectorEvents().doExecute(Collections.emptyMap(),
          content(new JSONObject()));

      verify(dal).rollbackAndClose();
      assertEquals("NullPointerException", result.getJSONObject("message").getString("text"),
          "an empty error toast tells the administrator nothing");
    }
  }

  private static String content(JSONObject params) throws Exception {
    return new JSONObject().put("_params", params).toString();
  }
}
