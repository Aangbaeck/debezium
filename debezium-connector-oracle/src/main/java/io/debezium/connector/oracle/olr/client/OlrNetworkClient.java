/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.ErrorReportConfiguration;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;

import jdk.net.ExtendedSocketOptions;

/**
 * An OpenLogReplicator network client that communicates using JSON streaming payloads.
 *
 * <p>The initial handshake is performed while the channel is in blocking mode as it consists of a
 * fixed request and response exchange. Once the server has entered streaming mode, the channel is
 * switched to non-blocking mode and {@link #readEvent()} waits at most {@link #READ_TIMEOUT} for a
 * complete message to arrive before returning {@code null}, buffering any partially received
 * message until the next call.
 *
 * <p>An interrupt raised while the client is communicating with the server is a request to shut
 * down rather than a network failure. Such an interrupt is handled internally by marking the client
 * as no longer connected, so that {@link #isConnected()} reports {@code false} and the caller's read
 * loop stops of its own accord instead of having to interpret a failure.
 *
 * <p>{@link #READ_TIMEOUT} only bounds a single poll and the caller polls again, so it never fails a
 * connection. OpenLogReplicator sends nothing while the database is quiet, possibly for hours, so silence
 * cannot tell a dead server from an idle one. TCP keepalive does: the operating system probes the idle
 * connection and fails it when the probes go unanswered or are refused, for example after the server's
 * pod was deleted without its connection being closed. The next read then throws an {@link IOException},
 * which the connector treats as retriable and reconnects.
 *
 * @author Chris Cranford
 */
public class OlrNetworkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlrNetworkClient.class);

    private static final int MESSAGE_SIZE_LENGTH = 4;
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(1);
    private static final int HEADER_SCAN_LENGTH = 256;
    private static final Pattern HEADER_POSITION = Pattern.compile("\"(scn|c_scn|c_idx)\":\"?([0-9A-Za-z]{1,32})");

    // A frame carries a 32-bit length, so no string in it can exceed Integer.MAX_VALUE; LOB values exceed Jackson's 20M default
    // Row values must not reach the logs through a parse error, so Jackson reports no source content and cuts tokens short
    private final ObjectMapper mapper = new ObjectMapper(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
            .errorReportConfiguration(ErrorReportConfiguration.builder()
                    .maxErrorTokenLength(0)
                    .maxRawContentLength(0)
                    .build())
            .build())
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final String hostName;
    private final int port;
    private final String sourceName;
    private final Duration keepAliveIdle;
    private final Duration keepAliveInterval;
    private final int keepAliveCount;
    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH).order(ByteOrder.LITTLE_ENDIAN);

    private SocketChannel channel;
    private Selector selector;
    private ByteBuffer payloadBuffer;
    private volatile boolean disconnected;
    private long prevScn;

    /**
     * Create the OpenLogReplicator network client.
     *
     * @param connectorConfig connector configuration
     */
    public OlrNetworkClient(OracleConnectorConfig connectorConfig) {
        this.hostName = connectorConfig.getOpenLogReplicatorHostname();
        this.port = connectorConfig.getOpenLogReplicatorPort();
        this.sourceName = connectorConfig.getOpenLogReplicatorSource();
        this.keepAliveIdle = connectorConfig.getOpenLogReplicatorKeepAliveIdle();
        this.keepAliveInterval = connectorConfig.getOpenLogReplicatorKeepAliveInterval();
        this.keepAliveCount = connectorConfig.getOpenLogReplicatorKeepAliveCount();
    }

    /**
     * Connect to the OpenLogReplicator process.
     *
     * @param scn the checkpoint commit to begin streaming from
     * @param index the checkpoint commit sequence index to begin streaming from
     * @return true if the connection was established, false if the connection failed
     */
    public boolean connect(Scn scn, Long index) {
        if (scn == null || scn.isNull()) {
            throw new OlrNetworkClientException("Cannot connect and start with a null system change number");
        }
        boolean connected = false;
        try {
            channel = SocketChannel.open();
            // Before connecting, so the handshake is covered too
            configureKeepAlive(channel);
            channel.configureBlocking(true);
            if (!channel.connect(new InetSocketAddress(hostName, port))) {
                return false;
            }

            if (!startFrom(scn, index)) {
                return false;
            }

            // The handshake completed and the server is streaming, all subsequent reads are
            // performed without blocking the caller.
            channel.configureBlocking(false);
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_READ);
            connected = true;
            return true;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
            return false;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to connect and start", e);
        }
        finally {
            // The caller only disconnects a client that connected
            if (!connected) {
                closeQuietly();
            }
        }
    }

    private void closeQuietly() {
        if (selector != null) {
            try {
                selector.close();
            }
            catch (IOException e) {
                // ignored
            }
            selector = null;
        }
        if (channel != null) {
            try {
                channel.close();
            }
            catch (IOException e) {
                // ignored
            }
        }
    }

    /**
     * Enables TCP keepalive on the channel with the configured timings, so a connection whose server
     * vanished fails within about idle + interval * count of its last traffic.
     *
     * <p>Where the per-socket timings are not supported the system-wide ones apply, which on Linux
     * default to probing only after two hours of idle time.
     */
    private void configureKeepAlive(SocketChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        final Set<SocketOption<?>> supported = channel.supportedOptions();
        if (!supported.contains(ExtendedSocketOptions.TCP_KEEPIDLE)
                || !supported.contains(ExtendedSocketOptions.TCP_KEEPINTERVAL)
                || !supported.contains(ExtendedSocketOptions.TCP_KEEPCOUNT)) {
            LOGGER.warn("Per-connection TCP keepalive timings are not supported on this platform, the operating system "
                    + "defaults apply to the OpenLogReplicator connection and may detect a lost server only after hours.");
            return;
        }
        channel.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, toSeconds(keepAliveIdle));
        channel.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, toSeconds(keepAliveInterval));
        channel.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, keepAliveCount);
        LOGGER.info("TCP keepalive on the OpenLogReplicator connection: idle {}s, interval {}s, count {}.",
                toSeconds(keepAliveIdle), toSeconds(keepAliveInterval), keepAliveCount);
    }

    private static int toSeconds(Duration duration) {
        // The socket options take whole seconds; round up so a configured value is never shortened
        return (int) Math.max(1, (duration.toMillis() + 999) / 1000);
    }

    @VisibleForTesting
    SocketChannel getChannel() {
        return channel;
    }

    /**
     * Disconnect from the OpenLogReplicator network service.
     */
    public void disconnect() {
        try {
            disconnected = true;
            if (selector != null) {
                try {
                    selector.close();
                }
                catch (Exception e) {
                    // ignored
                }
                selector = null;
            }
            if (channel.isOpen()) {
                try {
                    channel.shutdownInput();
                }
                catch (Exception e) {
                    // ignored
                }
                try {
                    channel.shutdownOutput();
                }
                catch (Exception e) {
                    // ignored
                }
                channel.close();
            }
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to disconnect client.", e);
        }
    }

    /**
     * Returns whether the network client is connected to the OpenLogReplicator process.
     *
     * @return true if the client is connected, false otherwise.
     */
    public boolean isConnected() {
        // The channel is only created by #connect and is cleared again by #disconnect, while this is
        // also called from the thread that flushes offsets, so it may be observed as unset.
        return !disconnected && channel != null && channel.isConnected();
    }

    /**
     * Reads the next streaming event from the OpenLogReplicator process, waiting at most
     * {@link #READ_TIMEOUT} for one to arrive.
     *
     * @return the streaming event, or {@code null} if no event became available
     */
    public StreamingEvent readEvent() throws OlrNetworkClientException {
        final StreamingEvent event = readNextEvent();
        if (event == null) {
            return null;
        }

        LOGGER.trace("Received Event: {}", event);
        return event;
    }

    public void confirm(Scn scn, Long index) {
        confirm(scn.longValue(), index);
    }

    private StreamingEvent readNextEvent() {
        final ByteBuffer message = read();
        if (message == null) {
            return null;
        }

        final String data = new String(message.array(), StandardCharsets.UTF_8);
        try {
            return mapper.readValue(data, StreamingEvent.class);
        }
        catch (JsonProcessingException e) {
            throw new OlrNetworkClientException("Failed to deserialize network packet: " + describePacket(data, message.limit(), e), e);
        }
    }

    /**
     * Describes where a packet that failed to deserialize is and where in it the error is, without
     * any of the row values it carries.
     */
    private static String describePacket(String data, int length, JsonProcessingException error) {
        final StringBuilder description = new StringBuilder();
        // The position fields precede the payload, so only the packet's head is searched for them
        int headerEnd = data.indexOf("\"payload\"");
        headerEnd = Math.min(headerEnd < 0 ? data.length() : headerEnd, HEADER_SCAN_LENGTH);
        final Matcher matcher = HEADER_POSITION.matcher(data).region(0, headerEnd);
        while (matcher.find()) {
            description.append(matcher.group(1)).append('=').append(matcher.group(2)).append(", ");
        }
        description.append("length=").append(length);
        final JsonLocation location = error.getLocation();
        if (location != null) {
            description.append(", offset=").append(location.getCharOffset())
                    .append(", line=").append(location.getLineNr())
                    .append(", column=").append(location.getColumnNr());
        }
        return description.append(": ").append(error.getOriginalMessage()).toString();
    }

    private void confirm(long newScn, Long index) {
        if (prevScn != 0 && prevScn < newScn && index != null) {
            LOGGER.debug("Confirming SCN {} with index {}", newScn, index);
            send(createRequest(RequestCode.CONFIRM).setCScn(newScn).setCIdx(index).build());
        }
        prevScn = newScn;
    }

    private boolean startFrom(Scn scn, Long index) {
        // No client-side filter by SCN: OpenLogReplicator sends a transaction on START only when it commits after the
        // start SCN, and on CONTINUE only after (c_scn, c_idx). A transaction open at the start SCN carries rows below it.
        if (index != null) {
            LOGGER.info("Streaming will start at SCN {} with index {}.", scn, index);
        }
        else {
            LOGGER.info("Streaming will start at SCN {}.", scn);
        }
        send(createRequest(RequestCode.INFO).build());

        RedoResponse response = readResponse();
        if (response == null) {
            return false;
        }
        else if (response.getCode() == ResponseCode.REPLICATE) {
            LOGGER.info("OpenLogReplicator has already started, continue from SCN {}", scn);
            // The position a source is continued from is carried by c_scn and c_idx. The system
            // change number on its own says where to begin reading redo, which only applies to
            // starting a source, and is ignored when continuing one. Sending it there leaves the
            // server to continue from whatever it last had confirmed instead, which is as far back
            // as it still holds, so it streams changes that were emitted long ago.
            send(createRequest(RequestCode.CONTINUE)
                    .setCScn(scn.longValue())
                    .setCIdx(index != null ? index : 0)
                    .build());
        }
        else if (response.getCode() == ResponseCode.READY) {
            // todo: add support for continue index (c_idx)??
            LOGGER.info("OpenLogReplicator ready, streaming from SCN {}.", scn);
            send(createRequest(RequestCode.START).setScn(scn.longValue()).build());
        }
        else {
            LOGGER.warn("Failed to get proper response from INFO request.");
            return false;
        }

        response = readResponse();
        if (response == null) {
            return false;
        }
        else if (response.getCode() != ResponseCode.REPLICATE) {
            LOGGER.warn("Server failed to enter streaming mode, OpenLogReplicator client shutting down.");
            return false;
        }

        LOGGER.info("OpenLogReplicator streaming client started successfully.");
        return true;
    }

    private RedoRequest.Builder createRequest(RequestCode requestCode) {
        return RedoRequest.newBuilder().setCode(requestCode).setDatabaseName(sourceName);
    }

    /**
     * Reads a handshake response from the server, while the channel is still in blocking mode.
     *
     * @return the response, or {@code null} if the client was interrupted awaiting the response
     */
    private RedoResponse readResponse() {
        final ByteBuffer response = read();
        if (response == null) {
            return null;
        }
        try {
            return RedoResponse.parseFrom(response.array());
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to read response", e);
        }
    }

    /**
     * Reads the next message from the channel, waiting up to {@link #READ_TIMEOUT} for the message
     * to arrive when the channel is in non-blocking mode.
     *
     * <p>A message that has only partially arrived within that window is retained across calls so
     * that the next read resumes where the previous one left off.
     *
     * @return the message payload, or {@code null} if the message has not been fully received
     */
    private ByteBuffer read() {
        final long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        while (true) {
            // Read the packet size
            if (payloadBuffer == null && fillBuffer(sizeBuffer)) {
                sizeBuffer.flip();
                payloadBuffer = ByteBuffer.allocate(sizeBuffer.getInt());
                sizeBuffer.clear();
            }

            // Read the packet
            if (payloadBuffer != null && fillBuffer(payloadBuffer)) {
                final ByteBuffer payload = payloadBuffer;
                payloadBuffer = null;
                payload.flip();
                return payload;
            }

            if (disconnected || !awaitData(deadline)) {
                return null;
            }
        }
    }

    /**
     * Waits for the channel to become readable, up to the supplied deadline.
     *
     * @param deadline the {@link System#nanoTime()} value at which the wait should give up
     * @return {@code true} if the channel should be read again, {@code false} if the deadline
     *         elapsed or the thread was interrupted
     */
    private boolean awaitData(long deadline) {
        if (Thread.currentThread().isInterrupted()) {
            markInterrupted();
            return false;
        }
        final long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            return false;
        }
        try {
            // Selector#select returns immediately when the thread is interrupted, which the next
            // pass detects above, so a shutdown request is never delayed by the timeout.
            selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            selector.selectedKeys().clear();
            return true;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to wait for data from OpenLogReplicator", e);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private int send(RedoRequest request) {
        try {
            // We need to write the size (4 bytes) plus the payload
            final ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH + request.getSerializedSize());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(request.getSerializedSize());
            buffer.put(request.toByteArray());
            buffer.flip();

            // A non-blocking channel is free to accept only part of the buffer per write
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return written;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
            return 0;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to send request to server", e);
        }
    }

    /**
     * Reads from the channel into the supplied buffer until the buffer is full or, when the channel
     * is in non-blocking mode, until the channel has no more data available.
     *
     * @param buffer the buffer to be filled, never {@code null}
     * @return {@code true} if the buffer was filled, {@code false} if no more data is currently
     *         available and the buffer remains partially filled
     */
    private boolean fillBuffer(ByteBuffer buffer) {
        try {
            while (buffer.hasRemaining()) {
                final int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    throw new OlrNetworkClientException("Connection lost");
                }
                if (bytesRead == 0) {
                    // The channel is non-blocking and there is nothing left to read for now,
                    // the buffer keeps whatever was read so the next call can resume from here.
                    return false;
                }
            }
            return true;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
            return false;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to fill byte buffer", e);
        }
    }

    /**
     * Records that the client was interrupted while communicating with OpenLogReplicator.
     *
     * <p>An interrupt is a request to shut down rather than a network failure, and it closes the
     * channel when raised by a blocking operation. The interrupt status is preserved and the client
     * marks itself as disconnected so that the caller's read loop ends without an exception having
     * to be raised and interpreted.
     */
    private void markInterrupted() {
        LOGGER.debug("Interrupted while communicating with OpenLogReplicator, marking client disconnected.");
        disconnected = true;
        Thread.currentThread().interrupt();
    }

}
