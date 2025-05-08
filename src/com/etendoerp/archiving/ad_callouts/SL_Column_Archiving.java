package com.etendoerp.archiving.ad_callouts;

import org.apache.commons.lang.StringUtils;
import org.hibernate.criterion.Restrictions;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.ad_callouts.SimpleCallout;
import org.openbravo.erpCommon.utility.Utility;
import org.openbravo.model.ad.datamodel.Column;

import javax.servlet.ServletException;

public class SL_Column_Archiving extends SimpleCallout {
  @Override
  protected void execute(CalloutInfo info) throws ServletException {
    String strIsKey = info.getStringParameter("inpiskey");
    String strTableId = info.getStringParameter("inpadTableId");

    if (StringUtils.equals(strIsKey, "Y")) {
      /*
      OBCriteria<Column> keyCriteria = OBDal.getInstance().createCriteria(Column.class);
      keyCriteria.add(Restrictions.eq(Column.PROPERTY_TABLE + ".id", strTableId));
      keyCriteria.add(Restrictions.eq(Column.PROPERTY_KEYCOLUMN, true));
      if (keyCriteria.count() != 0) {
        info.addResult("inpiskey", "N");
        info.showWarning(Utility.messageBD(this, "MultipleKeyColumns", info.vars.getLanguage())
            + keyCriteria.list().get(0).getDBColumnName());
      }
      */
    }
  }
}
