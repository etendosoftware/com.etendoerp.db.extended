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

import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.client.application.process.BaseProcessActionHandler;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.service.db.DalConnectionProvider;

/**
 * Puts the failed indexing events of one client, one source, or both back in the queue.
 *
 * <p>The scheduled {@link ProcessVectorRequeueFailed} requeues every failed event, which after
 * correcting one tenant's or one source's configuration would also resend the events of everyone
 * else that still fails for its own reason. This is the same decision, scoped. With neither
 * parameter it covers everything, exactly like the scheduled process.</p>
 */
public class RequeueFailedVectorEvents extends BaseProcessActionHandler {

  private static final Logger log = LogManager.getLogger();

  static final String CLIENT_PARAMETER = "AD_Client_ID";
  static final String SOURCE_PARAMETER = "ETARC_Vector_Source_ID";

  @Override
  protected JSONObject doExecute(Map<String, Object> parameters, String content) {
    JSONObject result = new JSONObject();
    try {
      JSONObject params = new JSONObject(content).getJSONObject("_params");
      VectorOutboxService outbox = new VectorOutboxService(new DalConnectionProvider(false),
          Collections.emptyList());
      int requeued = outbox.requeueFailed(ProcessVectorRequeueFailed.BATCH_SIZE,
          optionalId(params, CLIENT_PARAMETER), optionalId(params, SOURCE_PARAMETER));
      OBDal.getInstance().commitAndClose();
      result.put("message", message("success", OBMessageUtils.getI18NMessage(
          "ETARC_VectorFailedRequeued", new String[] { String.valueOf(requeued),
              String.valueOf(ProcessVectorRequeueFailed.BATCH_SIZE) })));
    } catch (Exception e) {
      log.error("Requeuing failed vector events failed", e);
      OBDal.getInstance().rollbackAndClose();
      try {
        result.put("message", message("error",
            e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
      } catch (Exception messageFailure) {
        log.error("Could not report the requeue failure", messageFailure);
      }
    }
    return result;
  }

  /**
   * Reads an optional selector parameter.
   *
   * @return the selected id, or {@code null} when the parameter was left empty, which the client
   *     sends either as a JSON null or as the string {@code "null"}
   */
  static String optionalId(JSONObject params, String name) {
    if (params == null || !params.has(name) || params.isNull(name)) {
      return null;
    }
    String value = params.optString(name, "").trim();
    return value.isEmpty() || "null".equals(value) ? null : value;
  }

  private static JSONObject message(String severity, String text) throws Exception {
    JSONObject message = new JSONObject();
    message.put("severity", severity);
    message.put("text", text);
    return message;
  }
}
