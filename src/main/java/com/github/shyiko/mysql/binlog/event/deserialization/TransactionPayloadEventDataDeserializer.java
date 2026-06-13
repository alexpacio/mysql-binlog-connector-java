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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
                    // Fetch the payload size
                    eventData.setPayloadSize(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size
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

        eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

        // Use streaming decompression to handle uncompressed sizes up to 4GB
        // This avoids hitting Java's 2GB array limit by processing events as they're decompressed
        ArrayList<Event> decompressedEvents = getDecompressedEvents(eventData);

        eventData.setUncompressedEvents(decompressedEvents);

        return eventData;
    }

    private ArrayList<Event> getDecompressedEvents(TransactionPayloadEventData eventData) throws IOException {
        ArrayList<Event> decompressedEvents = new ArrayList<>();
        EventDeserializer transactionPayloadEventDeserializer = new EventDeserializer();
        setCompatibilityMode(transactionPayloadEventDeserializer);

        try (InputStream decompressedInputStream = getDecompressedInputStream(eventData)) {
            ByteArrayInputStream destinationInputStream = new ByteArrayInputStream(decompressedInputStream);

            Event internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
            while(internalEvent != null) {
                decompressedEvents.add(internalEvent);
                internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
            }
        }
        return decompressedEvents;
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
}
