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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public interface CompressionService {

    /**
     * The compression type for this compression codec. @see org.apache.kafka.common.config.TopicConfig#COMPRESSION_TYPE_CONFIG
     */
    String type();

    public void checkAvailable();

    /**
     * Wrap bufferStream with an OutputStream that will compress data with this Compression.
     *
     * @param bufferStream The buffer to write the compressed data to
     */
    OutputStream compressedOutputStream(OutputStream bufferStream, Optional<Integer> compressionLevel);

    /**
     * Wrap buffer with an InputStream that will decompress data with this Compression.
     *
     * @param buffer The {@link ByteBuffer} instance holding the data to decompress.
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported.
     */
    InputStream decompressedInputStream(ByteBuffer buffer, BufferProvider decompressionBufferProvider);

    public interface BufferProvider {
        public ByteBuffer get(int capacity); 
        public void release(ByteBuffer buffer);
    }
}
