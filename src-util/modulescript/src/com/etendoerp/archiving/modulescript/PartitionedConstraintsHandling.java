package com.etendoerp.archiving.modulescript;

import java.sql.PreparedStatement;

import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

public class PartitionedConstraintsHandling extends ModuleScript {

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();

      // DROP
      StringBuilder sql = new StringBuilder("");

      //sql.append("ALTER TABLE IF EXISTS public.ad_audit_trail DROP CONSTRAINT IF EXISTS ad_audit_trail_pk;")
      //sql.append("ALTER TABLE IF EXISTS public.ad_audit_trail ADD CONSTRAINT ad_audit_trail_pk PRIMARY KEY (ad_audit_trail_id, created);\n");

      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_CFS_FACT_ACCT;\n");
      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_CFS_FACT_ACCT1;\n");
      //sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT\n");
      //sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_KEY;\n");
      //sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      //sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_CFS_KEY;\n");
      // ADD PK
      //sql.append("ALTER TABLE PUBLIC.FACT_ACCT\n");
      //sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_KEY;\n");
      //sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT\n");
      //sql.append("ADD CONSTRAINT FACT_ACCT_KEY PRIMARY KEY (FACT_ACCT_ID, DATEACCT);\n");
      //sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      //sql.append("ADD CONSTRAINT FACT_ACCT_CFS_KEY PRIMARY KEY (FACT_ACCT_CFS_ID, EM_ETABASE_DATEACCT);\n");
      // ADD FK
      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT FOREIGN KEY (FACT_ACCT_REF_ID, EM_ETABASE_DATEACCT_REF) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID, DATEACCT) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;\n");
      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT1 FOREIGN KEY (FACT_ACCT_ID, EM_ETABASE_DATEACCT) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID, DATEACCT) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE;\n");

      sql.append("CREATE OR REPLACE FUNCTION ETABASE_FACT_ACCT_CFS_REFD () RETURNS TRIGGER AS $$\n");
      sql.append("BEGIN\n");
      sql.append("SELECT f.DATEACCT\n");
      sql.append("INTO NEW.EM_ETABASE_DATEACCT_REF\n");
      sql.append("FROM fact_acct f\n");
      sql.append("WHERE f.FACT_ACCT_ID = NEW.FACT_ACCT_REF_ID;\n");
sql.append("\n");
      sql.append("RETURN NEW;\n");
      sql.append("END;\n");
      sql.append("$$ LANGUAGE PLPGSQL;\n");
sql.append("\n");
      sql.append("CREATE or replace TRIGGER ETABASE_FACT_ACCT_CFS_REFD BEFORE INSERT\n");
          sql.append("OR\n");
      sql.append("UPDATE ON PUBLIC.FACT_ACCT_CFS FOR EACH ROW\n");
      sql.append("EXECUTE FUNCTION ETABASE_FACT_ACCT_CFS_REFD ();\n");

      sql.append("CREATE OR REPLACE FUNCTION ETABASE_FACT_ACCT_CFS_DAT () RETURNS TRIGGER AS $$\n");
      sql.append("BEGIN\n");
      sql.append("SELECT f.DATEACCT\n");
      sql.append("INTO NEW.EM_ETABASE_DATEACCT\n");
      sql.append("FROM fact_acct f\n");
      sql.append("WHERE f.FACT_ACCT_ID = NEW.FACT_ACCT_ID;\n");
sql.append("\n");
      sql.append("RETURN NEW;\n");
      sql.append("END;\n");
      sql.append("$$ LANGUAGE PLPGSQL;\n");
sql.append("\n");
      sql.append("CREATE or replace TRIGGER ETABASE_FACT_ACCT_CFS_DAT BEFORE INSERT\n");
          sql.append("OR\n");
      sql.append("UPDATE ON PUBLIC.FACT_ACCT_CFS FOR EACH ROW\n");
      sql.append("EXECUTE FUNCTION ETABASE_FACT_ACCT_CFS_DAT ();\n");
sql.append("\n");

      PreparedStatement ps = cp.getPreparedStatement(
          sql.toString()
      );
      ps.executeUpdate();
    } catch (Exception e) {
      handleError(e);
    }
  }
}
