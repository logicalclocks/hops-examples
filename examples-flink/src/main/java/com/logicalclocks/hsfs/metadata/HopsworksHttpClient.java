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

import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

public interface HopsworksHttpClient {
  static final Logger LOGGER = null;

  <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException;

  String getTrustStorePath();

  String getKeyStorePath();

  String getCertKey();

  static String readCertKey(String materialPwd) {
    try {
      return FileUtils.readFileToString(new File(materialPwd));
    } catch (IOException ex) {
      LOGGER.warn("Failed to get cert password.", ex);
    }
    return null;
  }
}
