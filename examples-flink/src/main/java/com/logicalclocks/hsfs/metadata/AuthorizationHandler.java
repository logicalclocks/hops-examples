/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.metadata;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class AuthorizationHandler<T> implements ResponseHandler<T> {

  private ResponseHandler<T> originalResponseHandler;

  AuthorizationHandler(ResponseHandler<T> originalResponseHandler) {
    this.originalResponseHandler = originalResponseHandler;
  }

  AuthorizationHandler() {
  }

  @Override
  public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      throw new UnauthorizedException();
    } else if (response.getStatusLine().getStatusCode() / 100 == 4) {
      throw new IOException("Error: " + response.getStatusLine().getStatusCode()
          + EntityUtils.toString(response.getEntity(), Charset.defaultCharset()));
    } else if (response.getStatusLine().getStatusCode() / 100 == 5) {
      throw new InternalException("Error: " + response.getStatusLine().getStatusCode()
          + EntityUtils.toString(response.getEntity(), Charset.defaultCharset()));
    }

    if (originalResponseHandler != null) {
      return originalResponseHandler.handleResponse(response);
    }
    return null;
  }
}
