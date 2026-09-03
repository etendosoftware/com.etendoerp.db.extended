package com.etendoerp.db.extended.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Compiles the field/value subset of classic Display Logic to a parameterized metadata predicate. */
final class VectorDisplayLogicCompiler {
  private final String input;
  private final Map<String, String> fields;
  private int position;

  private VectorDisplayLogicCompiler(String input, Map<String, String> fields) {
    this.input = input == null ? "" : input.trim();
    this.fields = fields;
  }

  static VectorMetadataFilter compile(String expression, Map<String, String> fields) {
    if (expression == null || expression.trim().isEmpty()) return VectorMetadataFilter.jsonContains("{}");
    VectorDisplayLogicCompiler parser = new VectorDisplayLogicCompiler(expression, fields);
    Node root = parser.orExpression();
    parser.skipWhitespace();
    if (parser.position != parser.input.length()) parser.invalid("Unexpected token");
    List<String> parameters = new ArrayList<>();
    return VectorMetadataFilter.predicate(root.toSql(parameters), parameters);
  }

  private Node orExpression() {
    Node left = andExpression();
    while (consume('|')) left = new BinaryNode("OR", left, andExpression());
    return left;
  }

  private Node andExpression() {
    Node left = term();
    while (consume('&')) left = new BinaryNode("AND", left, term());
    return left;
  }

  private Node term() {
    if (consume('(')) {
      Node nested = orExpression();
      if (!consume(')')) invalid("Missing closing parenthesis");
      return nested;
    }
    return comparison();
  }

  private Node comparison() {
    skipWhitespace();
    if (!consume('@')) invalid("Expected a field token");
    int end = input.indexOf('@', position);
    if (end < 0) invalid("Unterminated field token");
    String token = input.substring(position, end);
    position = end + 1;
    String column = fields.get(token.toLowerCase(Locale.ROOT));
    if (column == null) invalid("Field '" + token + "' is not configured as vector metadata");
    skipWhitespace();
    String operator;
    if (consume('!')) {
      if (!consume('=')) invalid("Expected !=");
      operator = "<>";
    } else if (consume('=')) {
      operator = "=";
    } else {
      invalid("Expected = or !=");
      return null;
    }
    skipWhitespace();
    if (!consume('\'')) invalid("Expected a quoted literal");
    StringBuilder value = new StringBuilder();
    while (position < input.length() && input.charAt(position) != '\'') value.append(input.charAt(position++));
    if (!consume('\'')) invalid("Unterminated literal");
    return new ComparisonNode(column, operator, value.toString());
  }

  private boolean consume(char expected) {
    skipWhitespace();
    if (position < input.length() && input.charAt(position) == expected) {
      position++;
      return true;
    }
    return false;
  }

  private void skipWhitespace() { while (position < input.length() && Character.isWhitespace(input.charAt(position))) position++; }
  private void invalid(String detail) { throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA, "Invalid vector target Display Logic: " + detail + "."); }

  private interface Node { String toSql(List<String> parameters); }
  private static final class BinaryNode implements Node {
    private final String operator; private final Node left, right;
    BinaryNode(String operator, Node left, Node right) { this.operator = operator; this.left = left; this.right = right; }
    @Override public String toSql(List<String> parameters) { return "(" + left.toSql(parameters) + " " + operator + " " + right.toSql(parameters) + ")"; }
  }
  private static final class ComparisonNode implements Node {
    private final String column, operator, value;
    ComparisonNode(String column, String operator, String value) { this.column = column; this.operator = operator; this.value = value; }
    @Override public String toSql(List<String> parameters) { parameters.add(value); return "metadata -> 'fields' ->> '" + column + "' " + operator + " ?"; }
  }
}
