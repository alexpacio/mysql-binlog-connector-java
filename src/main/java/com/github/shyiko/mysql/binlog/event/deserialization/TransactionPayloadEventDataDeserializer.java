/*
 * Copyright 2013 Stanley Shyiko
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.luben.zstd.ZstdInputStream;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 * @author <a href="mailto:debjeet.sarkar@booking.com">Debjeet Sarkar</a>
 * @author <a href="mailto:pratik.pandey@booking.com">Pratik Pandey</a>
 */
public class TransactionPayloadEventDataDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
    public static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
    public static final int OTW_PAYLOAD_SIZE_FIELD = 1;
    public static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
    public static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;
    public static final int COMPRESSION_TYPE_ZSTD = 0;
    public static final int COMPRESSION_TYPE_NONE = 255;

    private EventDeserializer.CompatibilityMode[] compatibilityModes = new EventDeserializer.CompatibilityMode[0];

    void setCompatibilityMode(EnumSet<EventDeserializer.CompatibilityMode> compatibilitySet) {
        compatibilityModes = compatibilitySet.toArray(
            new EventDeserializer.CompatibilityMode[compatibilitySet.size()]
        );
    }

    @Override
    public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        TransactionPayloadEventData eventData = new TransactionPayloadEventData();
        eventData.setCompressionType(COMPRESSION_TYPE_NONE);
        // Read the header fields from the event data
        while (inputStream.available() > 0) {
            int fieldType = 0;
            int fieldLen = 0;
            // Read the type of the field
            if (inputStream.available() >= 1) {
                fieldType = inputStream.readPackedInteger();
            }
            // We have reached the end of the Event Data Header
            if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }
            // Read the size of the field
            if (inputStream.available() >= 1) {
                fieldLen = inputStream.readPackedInteger();
            }
            switch (fieldType) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    // Fetch the payload (compressed) size
                    eventData.setPayloadSize(readCompressedPayloadSize(inputStream));
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size (may exceed 2GB; inner events are streamed)
                    eventData.setUncompressedSize(inputStream.readPackedLong());
                    break;
                default:
                    // Ignore unrecognized field
                    inputStream.read(fieldLen);
                    break;
            }
        }
        if (eventData.getUncompressedSize() == 0) {
            // Default the uncompressed to the payload size
            eventData.setUncompressedSize(eventData.getPayloadSize());
        }

        // Fail fast on an unsupported compression type, before consuming the payload, so it
        // surfaces as a clean deserialization failure rather than later during inner-event streaming.
        requireSupportedCompressionType(eventData.getCompressionType());

        // The compressed payload must fit in a single byte[] (the binlog client reassembles the raw
        // event into one array regardless). Only the *uncompressed* size may exceed 2GB, which is why
        // inner events are decompressed and parsed lazily (see iterator()) instead of materialized.
        eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

        // Inner events are intentionally NOT materialized here: a compressed transaction is bounded
        // by the transaction size, not by any per-event limit, so eagerly parsing every inner event
        // into a list would put the whole transaction's object graph on the heap at once. They are
        // surfaced one at a time through iterator(); uncompressedEvents stays empty.
        return eventData;
    }

    /**
     * Opens a single-pass cursor over the transaction's inner events, decompressing the payload
     * incrementally. Peak memory is bounded by the compressed payload plus one inner event, so a
     * transaction whose uncompressed image exceeds the 2GB {@code byte[]} limit (or simply does not
     * fit in heap) is streamed through instead of being materialized whole. The returned iterator
     * must be {@link InnerEventIterator#close() closed} to release the native decompressor.
     */
    public InnerEventIterator iterator(TransactionPayloadEventData eventData) throws IOException {
        EventDeserializer innerEventDeserializer = new EventDeserializer();
        setCompatibilityMode(innerEventDeserializer);
        ByteArrayInputStream stream = new ByteArrayInputStream(getDecompressedInputStream(eventData));
        return new InnerEventIterator(innerEventDeserializer, stream);
    }

    private static int readCompressedPayloadSize(ByteArrayInputStream inputStream) throws IOException {
        long payloadSize = inputStream.readPackedLong();
        if (payloadSize > Integer.MAX_VALUE) {
            throw new IOException("Compressed transaction payload size " + payloadSize +
                " exceeds the maximum supported size of " + Integer.MAX_VALUE + " bytes");
        }
        return (int) payloadSize;
    }

    private static void requireSupportedCompressionType(int compressionType) throws IOException {
        if (compressionType != COMPRESSION_TYPE_ZSTD && compressionType != COMPRESSION_TYPE_NONE) {
            throw new IOException("Unsupported binlog_transaction_compression type: " +
                compressionType + " (only ZSTD and NONE are supported)");
        }
    }

    private InputStream getDecompressedInputStream(TransactionPayloadEventData eventData) throws IOException {
        InputStream payloadInputStream = new java.io.ByteArrayInputStream(eventData.getPayload());
        switch (eventData.getCompressionType()) {
            case COMPRESSION_TYPE_ZSTD:
                return new ZstdInputStream(payloadInputStream);
            case COMPRESSION_TYPE_NONE:
                return payloadInputStream;
            default:
                throw new IOException("Unsupported binlog_transaction_compression type: " +
                    eventData.getCompressionType() + " (only ZSTD and NONE are supported)");
        }
    }

    private void setCompatibilityMode(EventDeserializer eventDeserializer) {
        if (compatibilityModes.length > 0) {
            EventDeserializer.CompatibilityMode first = compatibilityModes[0];
            EventDeserializer.CompatibilityMode[] rest =
                new EventDeserializer.CompatibilityMode[compatibilityModes.length - 1];
            System.arraycopy(compatibilityModes, 1, rest, 0, rest.length);
            eventDeserializer.setCompatibilityMode(first, rest);
        }
    }

    /**
     * Single-pass cursor over the inner events of a {@link TransactionPayloadEventData}. Each
     * {@link #next()} decompresses and parses just enough of the payload to return one event;
     * {@code null} signals the end of the transaction, at which point the cursor closes itself.
     */
    public static final class InnerEventIterator implements Closeable {
        private EventDeserializer eventDeserializer;
        private ByteArrayInputStream inputStream;

        InnerEventIterator(EventDeserializer eventDeserializer, ByteArrayInputStream inputStream) {
            this.eventDeserializer = eventDeserializer;
            this.inputStream = inputStream;
        }

        /**
         * @return the next inner event, or {@code null} once the transaction is exhausted (which
         * also closes the cursor).
         * @throws IOException on a decompression or inner-event parse failure
         */
        public Event next() throws IOException {
            if (inputStream == null) {
                return null;
            }
            Event event = eventDeserializer.nextEvent(inputStream);
            if (event == null) {
                close();
            }
            return event;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                try {
                    // Closes the underlying (zstd) stream, releasing its native decompression context.
                    inputStream.close();
                } finally {
                    inputStream = null;
                    eventDeserializer = null;
                }
            }
        }
    }
}
