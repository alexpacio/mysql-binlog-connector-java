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
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;
import com.github.shyiko.mysql.binlog.network.SocketFactory;
import org.testng.annotations.Test;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogClientTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    @Test
    public void testEventListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getEventListeners().isEmpty());
        TraceEventListener traceEventListener = new TraceEventListener();
        binaryLogClient.registerEventListener(traceEventListener);
        binaryLogClient.registerEventListener(new CountDownEventListener());
        binaryLogClient.registerEventListener(new CapturingEventListener());
        assertEquals(binaryLogClient.getEventListeners().size(), 3);
        binaryLogClient.unregisterEventListener(traceEventListener);
        assertEquals(binaryLogClient.getEventListeners().size(), 2);
        binaryLogClient.unregisterEventListener(CapturingEventListener.class);
        assertEquals(binaryLogClient.getEventListeners().size(), 1);
    }

    @Test
    public void testLifecycleListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getLifecycleListeners().isEmpty());
        TraceLifecycleListener traceLifecycleListener = new TraceLifecycleListener();
        binaryLogClient.registerLifecycleListener(traceLifecycleListener);
        binaryLogClient.registerLifecycleListener(new BinaryLogClientStatistics());
        binaryLogClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
        });
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 3);
        binaryLogClient.unregisterLifecycleListener(traceLifecycleListener);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 2);
        binaryLogClient.unregisterLifecycleListener(BinaryLogClientStatistics.class);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 1);
    }

    @Test(expectedExceptions = TimeoutException.class)
    public void testNoConnectionTimeout() throws Exception {
        new BinaryLogClient("_localhost_", 3306, "root", "mysql").connect(0);
    }

    @Test(timeOut = 15000)
    public void testConnectionTimeout() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33059, "root", "mysql");
        final CountDownLatch socketBound = new CountDownLatch(1);
        final CountDownLatch binaryLogClientDisconnected = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33059));
                        socketBound.countDown();
                        Socket accept = serverSocket.accept();
                        accept.getOutputStream().write(1);
                        accept.getOutputStream().flush();
                        assertTrue(binaryLogClientDisconnected.await(3000, TimeUnit.MILLISECONDS));
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        binaryLogClient.setConnectTimeout(1000);
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            binaryLogClientDisconnected.countDown();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullEventDeserializerIsNotAllowed() throws Exception {
        new BinaryLogClient("localhost", 3306, "root", "mysql").setEventDeserializer(null);
    }

    @Test
    public void testTransactionPayloadNotifiesInnerEvents() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        final List<Event> notifiedEvents = new ArrayList<Event>();
        binaryLogClient.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                notifiedEvents.add(event);
            }
        });

        binaryLogClient.handleEvent(transactionPayloadEvent(500L, 12345L, xidEvent(111L), xidEvent(222L)));

        assertEquals(notifiedEvents.size(), 2);
        assertEquals(notifiedEvents.get(0).getHeader().getEventType(), EventType.XID);
        assertEquals(((XidEventData) notifiedEvents.get(0).getData()).getXid(), 111L);
        assertEquals(((XidEventData) notifiedEvents.get(1).getData()).getXid(), 222L);

        EventHeaderV4 firstHeader = notifiedEvents.get(0).getHeader();
        assertEquals(firstHeader.getPosition(), 11845L);
        assertEquals(firstHeader.getNextPosition(), 12345L);
        assertEquals(binaryLogClient.getBinlogPosition(), 12345L);
    }

    @Test
    public void testGtidSetAdvancesWhenCompressedTransactionCommitsInsidePayload() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        binaryLogClient.setGtidSet(SERVER_UUID + ":1-5");

        binaryLogClient.handleEvent(gtidEvent(6));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-5");

        binaryLogClient.handleEvent(transactionPayloadEvent(500L, 12345L, xidEvent(31L)));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-6");

        binaryLogClient.handleEvent(gtidEvent(7));
        binaryLogClient.handleEvent(transactionPayloadEvent(600L, 12945L, xidEvent(32L)));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-7");
    }

    @Test(timeOut = 15000)
    public void testDisconnectWhileBlockedByFBRead() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33061, "root", "mysql");
        final CountDownLatch readAttempted = new CountDownLatch(1);
        binaryLogClient.setSocketFactory(new SocketFactory() {
            @Override
            public Socket createSocket() throws SocketException {
                return new Socket() {

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new FilterInputStream(super.getInputStream()) {

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
                                readAttempted.countDown();
                                return super.read(b, off, len);
                            }
                        };
                    }
                };
            }
        });
        binaryLogClient.setKeepAlive(false);
        final CountDownLatch socketBound = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33061));
                        socketBound.countDown();
                        serverSocket.accept(); // accept socket but do NOT send anything
                        assertTrue(readAttempted.await(3000, TimeUnit.MILLISECONDS));
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.yield();
                                    binaryLogClient.disconnect();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                        thread.start();
                        thread.join();
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            assertEquals(readAttempted.getCount(), 0);
            assertTrue(e.getMessage().contains("Failed to connect to MySQL"));
        }
    }

    private Event gtidEvent(long transactionId) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.GTID);
        return new Event(header, new GtidEventData(
            MySqlGtid.fromString(SERVER_UUID + ":" + transactionId),
            (byte) 0, 0L, 0L, 0L, 0L, 0L, 0, 0
        ));
    }

    private Event xidEvent(long xid) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.XID);
        header.setEventLength(27L);
        header.setNextPosition(27L);
        XidEventData data = new XidEventData();
        data.setXid(xid);
        return new Event(header, data);
    }

    private Event transactionPayloadEvent(long eventLength, long nextPosition, Event... uncompressedEvents) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.TRANSACTION_PAYLOAD);
        header.setEventLength(eventLength);
        header.setNextPosition(nextPosition);
        TransactionPayloadEventData data = new TransactionPayloadEventData();
        data.setUncompressedEvents(new ArrayList<Event>(Arrays.asList(uncompressedEvents)));
        return new Event(header, data);
    }

    /*
    @Test
    public void testDeadlockyCode() throws IOException, InterruptedException {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "123456");
        binaryLogClient.setHeartbeatInterval(10000);
        binaryLogClient.setKeepAlive(true);
        binaryLogClient.setKeepAliveInterval(2000);

        binaryLogClient.connect();

        Thread.sleep(1000);

        binaryLogClient.disconnect();
    }
    */
}
