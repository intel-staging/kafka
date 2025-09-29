/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.compress;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;


public class PluggableCompression implements Compression {

    private Integer level;
    private CompressionService service;
    private static HashMap<String, Compression.Builder<? extends Compression>> builders = new HashMap<>();

    public PluggableCompression(CompressionService service, int level) {
        this.level = level; 
        this.service = service; 
    }

    public PluggableCompression(CompressionService service) {
        this.service = service; 
    }

    @Override
    public CompressionType type() {
        return CompressionType.forName(service.type());
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            return service.compressedOutputStream(buffer, Optional.ofNullable(level));
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            return service.decompressedInputStream(buffer, new CompressionService.BufferProvider() {
                    public ByteBuffer get(int capacity) {
                        return decompressionBufferSupplier.get(capacity);
                    }
                    public void release(ByteBuffer buffer) {
                        decompressionBufferSupplier.release(buffer);
                    }
            });
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public int decompressionOutputSize() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluggableCompression that = (PluggableCompression) o;
        if (that.service == null || service.getClass() != that.getClass()) return false;
        return level.equals(that.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static void loadService(String cls, CompressionType type) {
        ServiceLoader<CompressionService> serviceSelector = ServiceLoader.load(CompressionService.class);
        for (CompressionService service : serviceSelector) {
            if (service.getClass().getName().equals(cls) && service.type().equals(type.toString())) {
                service.checkAvailable();
                builders.put(service.type(), new PluggableCompression.Builder(service));
                return;
            }
        }
    }

    public static ConfigDef.Validator providerValidator() {
        return new ConfigDef.Validator() {
            @Override
            public void ensureValid(String name, Object o) {
                    if (o == null) return;
                    if (((String) o).isEmpty()) return;
                    try {
                        Class.forName((String) o);
                    } catch (ClassNotFoundException e) {
                        throw new ConfigException(name, o, "The class was not found on the classpath");
                    }
                    final CompressionType type;
                    switch (name) {
                        case "compression.gzip.provider" : type = CompressionType.GZIP; 
                            break; 
                        case "compression.zstd.provider" : type = CompressionType.ZSTD; 
                            break;
                        default: throw new ConfigException(name, "Invalid config");
                    }
                    org.apache.kafka.common.compress.PluggableCompression.loadService((String) o, type);
            }
        };
    }

    public static Compression.Builder<? extends Compression> builder(String typeName) {
        return builders.get(typeName);
    }

    public static class Builder implements Compression.Builder<PluggableCompression> {
        private int level;
        private CompressionService service;

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public Builder(CompressionService service) {
            this.service = service;
        }

        @Override
        public PluggableCompression build() {
            return new PluggableCompression(service, level);
        }
    }

}
