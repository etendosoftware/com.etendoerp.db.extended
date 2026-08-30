package com.etendoerp.db.extended.vector;

/** Controlled exception for vector capability and validation failures. */
public class VectorException extends RuntimeException {
  private final VectorErrorCode code;
  public VectorException(VectorErrorCode code, String message) { super(message); this.code = code; }
  public VectorException(VectorErrorCode code, String message, Throwable cause) { super(message, cause); this.code = code; }
  public VectorErrorCode getCode() { return code; }
}
