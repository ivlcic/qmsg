package com.example.batch.common;

import com.dropchop.recyclone.base.api.model.invoke.ErrorCode;
import com.dropchop.recyclone.base.api.model.invoke.ServiceException;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 02. 07. 2026.
 */
public class BatchServiceException extends ServiceException {

  public BatchServiceException(String message) {
    this(ErrorCode.process_error, message);
  }

  public BatchServiceException(String message, Throwable cause) {
    this(ErrorCode.process_error, message, cause);
  }

  public BatchServiceException(ErrorCode code, String message) {
    super(code, message);
  }

  public BatchServiceException(ErrorCode code, String message, Throwable cause) {
    super(code, message, cause);
  }
}
