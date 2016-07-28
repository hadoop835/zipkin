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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.Nullable;
import zipkin.storage.guava.GuavaSpanConsumer;

import static zipkin.storage.cassandra.CassandraUtil.bindWithName;
import static zipkin.storage.cassandra.CassandraUtil.durationIndexBucket;
import static com.google.common.util.concurrent.Futures.transform;

final class CassandraSpanConsumer implements GuavaSpanConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSpanConsumer.class);

  private static final Function<Object, Void> TO_VOID = Functions.<Void>constant(null);

  private final Session session;
  private final PreparedStatement insertSpan;
  private final PreparedStatement insertServiceSpanName;
  private final Schema.Metadata metadata;
  private final ObjectMapper mapper = new ObjectMapper();

  CassandraSpanConsumer(Session session, int bucketCount, @Nullable CacheBuilderSpec indexCacheSpec) {
    this.session = session;
    this.metadata = Schema.readMetadata(session);

    insertSpan = session.prepare(
        QueryBuilder
            .insertInto(Tables.TRACES)
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("id", QueryBuilder.bindMarker("id"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))
            .value("parent_id", QueryBuilder.bindMarker("parent_id"))
            .value("duration", QueryBuilder.bindMarker("duration"))
            .value("annotations", QueryBuilder.bindMarker("annotations"))
            .value("binary_annotations", QueryBuilder.bindMarker("binary_annotations"))
            .value("all_annotations", QueryBuilder.bindMarker("all_annotations")));

    insertServiceSpanName = session.prepare(
        QueryBuilder
            .insertInto(Tables.SERVICE_SPAN_NAMES)
            .value("service_name", QueryBuilder.bindMarker("service_name"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))
            .value("bucket", QueryBuilder.bindMarker("bucket"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))
            .value("duration", QueryBuilder.bindMarker("duration")));

    MappingManager mapping = new MappingManager(session);
    TypeCodec<EndpointUDT> endpointCodec = mapping.udtCodec(EndpointUDT.class);
    TypeCodec<AnnotationUDT> annoCodec = mapping.udtCodec(AnnotationUDT.class);
    TypeCodec<BinaryAnnotationUDT> bAnnoCodec = mapping.udtCodec(BinaryAnnotationUDT.class);
    KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());

    session.getCluster().getConfiguration().getCodecRegistry()
        .register(new TypeCodecImpl(keyspace.getUserType("endpoint"), EndpointUDT.class, endpointCodec))
        .register(new TypeCodecImpl(keyspace.getUserType("annotation"), AnnotationUDT.class, annoCodec))
        .register(new TypeCodecImpl(keyspace.getUserType("binary_annotation"), BinaryAnnotationUDT.class, bAnnoCodec));
  }

  /**
   * This fans out into many requests, last count was 8 * spans.size. If any of these fail, the
   * returned future will fail. Most callers drop or log the result.
   */
  @Override
  public ListenableFuture<Void> accept(List<Span> rawSpans) {
    ImmutableSet.Builder<ListenableFuture<?>> futures = ImmutableSet.builder();

    ImmutableList.Builder<Span> spans = ImmutableList.builder();
    for (Span rawSpan : rawSpans) {
      // indexing occurs by timestamp, so derive one if not present.
      Span span = ApplyTimestampAndDuration.apply(rawSpan);
      spans.add(span);

      futures.add(storeSpan(span));

      for (String serviceName : span.serviceNames()) {
        // QueryRequest.min/maxDuration
        if (span.timestamp != null && span.duration != null) {
          // Contract for Repository.storeServiceSpanName is to store the span twice, once with
          // the span name and another with empty string.
          futures.add(storeServiceSpanName(serviceName, span.name, span.timestamp, span.duration, span.traceId));
          if (!span.name.isEmpty()) { // If span.name == "", this would be redundant
            futures.add(storeServiceSpanName(serviceName, "", span.timestamp, span.duration, span.traceId));
          }
        }
      }
    }
    return transform(Futures.allAsList(futures.build()), TO_VOID);
  }

  /**
   * Store the span in the underlying storage for later retrieval.
   */
  ListenableFuture<?> storeSpan(Span span) {
    try {
      if (0 == span.timestamp && metadata.compactionClass.contains("TimeWindowCompactionStrategy")) {
        LOG.warn("Span {} in trace {} had no timestamp. "
            + "If this happens a lot consider switching back to SizeTieredCompactionStrategy for "
            + "{}.traces", span.id, span.traceId, session.getLoggedKeyspace());
      }

      List<AnnotationUDT> annotations = new ArrayList<>(span.annotations.size());
      for (Annotation annotation : span.annotations) {
          annotations.add(new AnnotationUDT(annotation));
      }
      List<BinaryAnnotationUDT> binaryAnnotations = new ArrayList<>(span.binaryAnnotations.size());
      for (BinaryAnnotation annotation : span.binaryAnnotations) {
          binaryAnnotations.add(new BinaryAnnotationUDT(annotation));
      }

      List<Object> all_annotations = new ArrayList<>();
      all_annotations.addAll(span.annotations);
      all_annotations.addAll(span.binaryAnnotations);

      BoundStatement bound = bindWithName(insertSpan, "insert-span")
          .setVarint("trace_id", BigInteger.valueOf(span.traceId))
          .setTimestamp("ts", new Date(span.timestamp))
          .setLong("id", span.id)
          .setString("span_name", span.name)
          .setLong("duration", span.duration)
          .setList("annotations", annotations)
          .setList("binary_annotations", binaryAnnotations)
          .setString("all_annotations", mapper.writeValueAsString(all_annotations));

      if (null != span.parentId) {
          bound = bound.setLong("parent_id", span.parentId);
      }

      return session.executeAsync(bound);
    } catch (RuntimeException | IOException ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }

  ListenableFuture<?> storeServiceSpanName(
          String serviceName,
          String spanName,
          long timestamp,
          long duration,
          long traceId) {

    int bucket = durationIndexBucket(timestamp);
    try {
      BoundStatement bound =
          bindWithName(insertServiceSpanName, "insert-service-span-name")
              .setString("service_name", serviceName)
              .setString("span_name", spanName)
              .setInt("bucket", bucket)
              .setUUID("ts", new UUID(
                      UUIDs.startOf(timestamp).getMostSignificantBits(),
                      UUIDs.timeBased().getLeastSignificantBits()))
              .setLong("duration", duration)
              .setVarint("trace_id", BigInteger.valueOf(traceId));

      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }

  /** Clears any caches */
  @VisibleForTesting void clear() {
  }

  @UDT(keyspace = "zipkin", name = "endpoint")
  static class EndpointUDT {

    private String service_name;
    private InetAddress ipv4;
    private byte[] ipv6;
    private int port;

    public EndpointUDT() {
        this.service_name = null;
        this.ipv4 = null;
        this.ipv6 = null;
        this.port = 0;
    }

    EndpointUDT(Endpoint endpoint) {
        this.service_name = endpoint.serviceName;
        this.ipv4 = from(endpoint.ipv4);
        this.ipv6 = endpoint.ipv6;
        this.port = endpoint.port;
    }
    private static InetAddress from(int ipv4) {
        try {
            return Inet4Address.getByAddress(BigInteger.valueOf(ipv4).toByteArray());
        } catch (UnknownHostException ex) {
            return null;
        }
    }

        public String getService_name() {
            return service_name;
        }

        public InetAddress getIpv4() {
            return ipv4;
        }

        public byte[] getIpv6() {
            return ipv6;
        }

        public int getPort() {
            return port;
        }

        public void setService_name(String service_name) {
            this.service_name = service_name;
        }

        public void setIpv4(InetAddress ipv4) {
            this.ipv4 = ipv4;
        }

        public void setIpv6(byte[] ipv6) {
            this.ipv6 = ipv6;
        }

        public void setPort(int port) {
            this.port = port;
        }

  }

  @UDT(keyspace = "zipkin", name = "annotation")
  static class AnnotationUDT {

        private long ts;
        private String v;
        private EndpointUDT ep;

        AnnotationUDT() {
            this.ts = 0;
            this.v = null;
            this.ep = null;
        }

        AnnotationUDT(Annotation annotation) {
            this.ts = annotation.timestamp;
            this.v = annotation.value;
            this.ep = new EndpointUDT(annotation.endpoint);
        }

        public long getTs() {
            return ts;
        }

        public String getV() {
            return v;
        }

        public EndpointUDT getEp() {
            return ep;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public void setV(String v) {
            this.v = v;
        }

        public void setEp(EndpointUDT ep) {
            this.ep = ep;
        }

  }

  @UDT(keyspace = "zipkin", name = "binary_annotation")
  static class BinaryAnnotationUDT {

        private String k;
        private byte[] v;
        private String t;
        private EndpointUDT ep;

        BinaryAnnotationUDT() {
            this.k = null;
            this.v = null;
            this.t = null;
            this.ep = null;
        }

        BinaryAnnotationUDT(BinaryAnnotation annotation) {
            this.k = annotation.key;
            this.v = annotation.value;
            this.t = annotation.type.name();
            this.ep = new EndpointUDT(annotation.endpoint);
        }

        public String getK() {
            return k;
        }

        public byte[] getV() {
            return v;
        }

        public String getT() {
            return t;
        }

        public EndpointUDT getEp() {
            return ep;
        }

        public void setK(String k) {
            this.k = k;
        }

        public void setV(byte[] v) {
            this.v = v;
        }

        public void setT(String t) {
            this.t = t;
        }

        public void setEp(EndpointUDT ep) {
            this.ep = ep;
        }
    }

    private static class TypeCodecImpl<T> extends TypeCodec<T> {

        private final TypeCodec<T> codec;

        public TypeCodecImpl(DataType cqlType, Class<T> javaClass, TypeCodec<T> codec) {
            super(cqlType, javaClass);
            this.codec = codec;
        }
        @Override
        public ByteBuffer serialize(T t, ProtocolVersion pv) throws InvalidTypeException {
            return codec.serialize(t, pv);
        }
        @Override
        public T deserialize(ByteBuffer bb, ProtocolVersion pv) throws InvalidTypeException {
            return codec.deserialize(bb, pv);
        }
        @Override
        public T parse(String string) throws InvalidTypeException {
            return codec.parse(string);
        }
        @Override
        public String format(T t) throws InvalidTypeException {
            return codec.format(t);
        }
    }
}
