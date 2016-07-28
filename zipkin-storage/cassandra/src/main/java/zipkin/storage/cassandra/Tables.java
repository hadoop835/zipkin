/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.cassandra;

import zipkin.Endpoint;
import zipkin.storage.SpanStore;

final class Tables {


  static final String TRACES = "traces";

  /**
   * This index supports {@link SpanStore#getServiceNames()}}.
   *
   * <p>The cardinality of {@link Endpoint#serviceName} values is expected to be stable and low.
   * There may be hot partitions, but each partition only includes a single column. Hence bucketing
   * would be unnecessary.
   */
  static final String SERVICE_NAMES = "service_names";


  static final String SERVICE_SPAN_NAMES = "service_span_name_index";


  private Tables() {
  }
}
