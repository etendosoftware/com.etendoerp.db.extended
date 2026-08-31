package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.openbravo.dal.core.OBContext;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

class VectorSearchContextTest {

  @Test
  void requiresAnActiveClientContext() {
    try (MockedStatic<OBContext> contextApi = mockStatic(OBContext.class)) {
      contextApi.when(OBContext::getOBContext).thenReturn(null);

      VectorException failure = assertThrows(VectorException.class, VectorSearchContext::current);

      assertEquals(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED, failure.getCode());
    }
  }

  @Test
  void resolvesClientAndOrganizationOnlyFromTheActiveContext() {
    OBContext obContext = mock(OBContext.class);
    Client client = mock(Client.class);
    Organization organization = mock(Organization.class);
    when(obContext.getCurrentClient()).thenReturn(client);
    when(obContext.getCurrentOrganization()).thenReturn(organization);
    when(client.getId()).thenReturn("CLIENT-A");
    when(organization.getId()).thenReturn("ORG-A");

    try (MockedStatic<OBContext> contextApi = mockStatic(OBContext.class)) {
      contextApi.when(OBContext::getOBContext).thenReturn(obContext);

      VectorSearchContext context = VectorSearchContext.current();

      assertEquals("CLIENT-A", context.getClientId());
      assertEquals("ORG-A", context.getOrganizationId());
    }
  }
}
