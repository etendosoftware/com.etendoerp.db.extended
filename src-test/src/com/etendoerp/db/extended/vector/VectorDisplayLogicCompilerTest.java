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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Covers the Display Logic a search target is filtered by.
 *
 * <p>An administrator writes this expression in a window and it ends up inside the SQL of every
 * search on that target. Two things therefore matter more than the parsing itself: a value never
 * reaches the statement as text, and a field nobody configured as metadata is refused rather than
 * written into it.</p>
 */
class VectorDisplayLogicCompilerTest {

  private static final Map<String, String> FIELDS =
      Map.of("docstatus", "docstatus", "salesrep", "salesrep_id", "issotrx", "issotrx");

  @Test
  void bindsEveryValueInsteadOfWritingItIntoTheStatement() throws Exception {
    VectorMetadataFilter filter = compile("@docstatus@='CO'");

    assertEquals(List.of("CO"), bound(filter));
    assertFalse(filter.getClause().contains("CO"),
        "the value has to travel as a parameter: an administrator types this in a window, and "
            + "concatenating it would put whatever they typed into the search statement");
    assertTrue(filter.getClause().contains("metadata -> 'fields' ->> 'docstatus' = ?"));
  }

  @Test
  void bindsAValueBuiltToBreakOutOfTheStatement() throws Exception {
    VectorMetadataFilter filter = compile("@docstatus@='CO OR 1=1 --'");

    assertEquals(List.of("CO OR 1=1 --"), bound(filter), "it stays one value");
    assertFalse(filter.getClause().contains("OR 1=1"), "and never becomes part of the statement");
  }

  @Test
  void refusesALiteralCarryingAQuoteRatherThanCuttingItShort() {
    // The subset has no escape: a literal ends at the first quote. Reading it as an empty value
    // and ignoring the rest would silently filter on something nobody wrote, so the trailing text
    // is what makes the whole expression invalid.
    assertRejects("@docstatus@='O'Brien'", "a value with a quote in it cannot be written here");
  }

  @Test
  void refusesAFieldNobodyConfiguredAsMetadata() {
    VectorException failure = assertThrows(VectorException.class, () -> compile("@password@='x'"));

    assertEquals(VectorErrorCode.VECTOR_INVALID_METADATA, failure.getCode(),
        "the field name is written into the statement, so only the configured ones may be used");
  }

  @Test
  void matchesTheFieldTokenWhateverItsCase() {
    assertTrue(compile("@DocStatus@='CO'").getClause().contains("->> 'docstatus'"),
        "Display Logic elsewhere in the application is not case sensitive about the token");
  }

  @Test
  void bindsBothSidesOfACombinedExpression() throws Exception {
    VectorMetadataFilter filter = compile("@docstatus@='CO' & @issotrx@='Y'");

    assertEquals(List.of("CO", "Y"), bound(filter), "in the order they are read");
    assertTrue(filter.getClause().contains(" AND "));
  }

  @Test
  void readsAndAsBindingTighterThanOr() {
    String clause = compile("@docstatus@='CO' | @docstatus@='CL' & @issotrx@='Y'").getClause();

    assertTrue(clause.matches(".*OR \\(.*AND.*\\).*"),
        "the AND groups first, as it does everywhere else: read the other way round, a target "
            + "would return records the administrator meant to exclude");
  }

  @Test
  void honoursParenthesesOverPrecedence() {
    String clause = compile("(@docstatus@='CO' | @docstatus@='CL') & @issotrx@='Y'").getClause();

    assertTrue(clause.matches(".*\\(.*OR.*\\) AND.*"));
  }

  @Test
  void translatesInequalityToWhatPostgresUnderstands() {
    assertTrue(compile("@docstatus@!='VO'").getClause().contains("<> ?"));
  }

  @Test
  void filtersNothingWhenNoExpressionWasWritten() throws Exception {
    for (String empty : new String[] { null, "", "   " }) {
      VectorMetadataFilter filter = VectorDisplayLogicCompiler.compile(empty, FIELDS);

      assertEquals(List.of("{}", "{}"), bound(filter),
          "a target with no Display Logic matches its whole namespace, and says so with an empty "
              + "json rather than with no predicate at all");
      assertFalse(filter.getClause().contains("metadata -> 'fields'"));
    }
  }

  @Test
  void refusesAnExpressionItCannotFullyRead() {
    assertRejects("@docstatus@='CO' garbage", "trailing text would otherwise be ignored in silence");
    assertRejects("@docstatus@", "a token with no comparison says nothing");
    assertRejects("@docstatus='CO'", "an unterminated field token");
    assertRejects("@docstatus@ 'CO'", "no operator at all");
    assertRejects("@docstatus@>'CO'", "an operator this subset does not implement");
    assertRejects("@docstatus@=CO", "an unquoted literal");
    assertRejects("(@docstatus@='CO'", "a parenthesis that was never closed");
  }

  private static void assertRejects(String expression, String why) {
    VectorException failure = assertThrows(VectorException.class, () -> compile(expression), why);
    assertEquals(VectorErrorCode.VECTOR_INVALID_METADATA, failure.getCode(), why);
  }

  /** The values as the filter really hands them to a statement. */
  private static List<String> bound(VectorMetadataFilter filter) throws Exception {
    List<String> values = new ArrayList<>();
    PreparedStatement statement = mock(PreparedStatement.class);
    doAnswer(invocation -> values.add(invocation.getArgument(1)))
        .when(statement).setString(anyInt(), anyString());

    int next = filter.bind(statement, 1);

    assertEquals(1 + values.size(), next,
        "bind has to report the next free index, or whatever the caller binds after it lands on "
            + "the wrong parameter");
    return values;
  }

  private static VectorMetadataFilter compile(String expression) {
    return VectorDisplayLogicCompiler.compile(expression, FIELDS);
  }
}
