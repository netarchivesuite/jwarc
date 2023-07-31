/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2018-2023 National Library of Australia and the jwarc contributors
 */

package org.netpreserve.jwarc;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.*;

/**
 * Writes records to a WARC file.
 */
public class WarcWriter implements Closeable {
    private static final byte[] TRAILER = new byte[]{'\r', '\n', '\r', '\n'};
    private final WritableByteChannel channel;
    private final WarcCompression compression;
    private final ByteBuffer buffer = ByteBuffer.allocate(8192);
    private final String digestAlgorithm = "SHA-1";
    private final AtomicLong position = new AtomicLong(0);
    private final Set<Socket> fetchSockets = Collections.synchronizedSet(new HashSet<>());
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    private volatile boolean closing = false;

    public WarcWriter(WritableByteChannel channel, WarcCompression compression) throws IOException {
        this.compression = compression;
        if (compression == WarcCompression.GZIP) {
            this.channel = new GzipChannel(channel);
        } else {
            this.channel = channel;
        }

        if (channel instanceof SeekableByteChannel) {
            position.set(((SeekableByteChannel) channel).position());
        }
    }

    public WarcWriter(WritableByteChannel channel) throws IOException {
        this(channel, WarcCompression.NONE);
    }

    public WarcWriter(OutputStream stream) throws IOException {
        this(Channels.newChannel(stream));
    }

    /**
     * Opens a WARC file for writing. Compression is determined by the file extension.
     *
     * @param path the path to the file
     * @throws IOException if an I/O error occurs
     */
    public WarcWriter(Path path) throws IOException {
        this(FileChannel.open(path, WRITE, CREATE, TRUNCATE_EXISTING), WarcCompression.forPath(path));
    }

    public synchronized void write(WarcRecord record) throws IOException {
        // TODO: buffer headers
        position.addAndGet(channel.write(ByteBuffer.wrap(record.serializeHeader())));
        MessageBody body = record.body();
        while (body.read(buffer) >= 0) {
            buffer.flip();
            position.addAndGet(channel.write(buffer));
            buffer.compact();
        }
        position.addAndGet(channel.write(ByteBuffer.wrap(TRAILER)));
        if (compression == WarcCompression.GZIP) {
            ((GzipChannel) channel).finish();
            position.set(((GzipChannel) channel).outputPosition());
        }
    }

    /**
     * Downloads a remote resource recording the request and response as WARC records.
     */
    public FetchResult fetch(URI uri) throws IOException {
        return fetch(uri, new FetchOptions());
    }

    /**
     * Downloads a remote resource recording the request and response as WARC records.
     * <p>
     * @param uri URL to download
     * @param options fetch options to use
     * @throws IOException if an IO error occurred
     */
    public FetchResult fetch(URI uri, FetchOptions options) throws IOException {
        HttpRequest httpRequest = new HttpRequest.Builder("GET", uri)
                .version(MessageVersion.HTTP_1_0) // until we support chunked encoding
                .addHeader("User-Agent", options.userAgent)
                .addHeader("Connection", "close")
                .build();
        return fetch(uri, httpRequest, options);
    }

    /**
     * Downloads a remote resource recording the request and response as WARC records.
     * <p>
     * @param uri URL to download
     * @param httpRequest request to send
     * @param copyTo if not null will receive a copy of the (raw) http response bytes
     * @throws IOException if an IO error occurred
     */
    public FetchResult fetch(URI uri, HttpRequest httpRequest, OutputStream copyTo) throws IOException {
        return fetch(uri, httpRequest, new FetchOptions().copyTo(copyTo));
    }

    /**
     * Downloads a remote resource recording the request and response as WARC records.
     * <p>
     * @param uri URL to download
     * @param httpRequest request to send
     * @param options fetch options to use
     * @throws IOException if an IO error occurred
     */
    public FetchResult fetch(URI uri, HttpRequest httpRequest, FetchOptions options) throws IOException {
        Exception exception = null;
        Path tempPath = Files.createTempFile("jwarc", ".tmp");
        closeLock.readLock().lock();
        try (FileChannel tempFile = FileChannel.open(tempPath, READ, WRITE, DELETE_ON_CLOSE, TRUNCATE_EXISTING)) {
            byte[] httpRequestBytes = httpRequest.serializeHeader();
            MessageDigest requestBlockDigest = MessageDigest.getInstance(digestAlgorithm);
            requestBlockDigest.update(httpRequestBytes);

            MessageDigest responseBlockDigest = MessageDigest.getInstance(digestAlgorithm);
            InetAddress ip = null;
            Instant date = Instant.now();
            long startMillis = date.toEpochMilli();
            WarcTruncationReason truncationReason = null;
            long totalLength = 0;
            try (Socket socket = IOUtils.connect(uri.getScheme(), uri.getHost(), uri.getPort())) {
                fetchSockets.add(socket);
                try {
                    if (closing) throw new IOException("WarcWriter closed");
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(options.readTimeout);
                    ip = ((InetSocketAddress) socket.getRemoteSocketAddress()).getAddress();
                    socket.getOutputStream().write(httpRequestBytes);
                    InputStream inputStream = socket.getInputStream();
                    byte[] buf = new byte[8192];
                    while (true) {
                        int len;
                        if (options.maxLength > 0 && options.maxLength - totalLength < buf.length) {
                            len = (int) (options.maxLength - totalLength);
                        } else {
                            len = buf.length;
                        }
                        int n = inputStream.read(buf, 0, len);
                        if (n < 0) break;
                        totalLength += n;
                        tempFile.write(ByteBuffer.wrap(buf, 0, n));
                        responseBlockDigest.update(buf, 0, n);
                        try {
                            if (options.copyTo != null) options.copyTo.write(buf, 0, n);
                        } catch (IOException e) {
                            // ignore
                        }
                        if (options.maxTime > 0 && System.currentTimeMillis() - startMillis > options.maxTime) {
                            truncationReason = WarcTruncationReason.TIME;
                            break;
                        }
                        if (options.maxLength > 0 && totalLength >= options.maxLength) {
                            truncationReason = WarcTruncationReason.LENGTH;
                            break;
                        }
                    }
                } catch (SocketException e) {
                    if (!closing || totalLength == 0) throw e;
                    truncationReason = WarcTruncationReason.UNSPECIFIED;
                    exception = e;
                } finally {
                    fetchSockets.remove(socket);
                }
            }

            tempFile.position(0);
            MessageDigest responsePayloadDigest = tryCalculatingPayloadDigest(tempFile);

            tempFile.position(0);
            WarcResponse.Builder responseBuilder = new WarcResponse.Builder(uri)
                    .blockDigest(new WarcDigest(responseBlockDigest))
                    .date(date)
                    .body(MediaType.HTTP_RESPONSE, tempFile, tempFile.size());
            if (ip != null) responseBuilder.ipAddress(ip);
            if (responsePayloadDigest != null) {
                responseBuilder.payloadDigest(new WarcDigest(responsePayloadDigest));
            }
            if (truncationReason != null) responseBuilder.truncated(truncationReason);
            WarcResponse response = responseBuilder.build();
            response.http(); // force HTTP header to be parsed before body is consumed so that caller can use it
            write(response);
            WarcRequest request = new WarcRequest.Builder(uri)
                    .blockDigest(new WarcDigest(requestBlockDigest))
                    .date(date)
                    .body(httpRequest)
                    .concurrentTo(response.id())
                    .build();
            request.http(); // force HTTP header to be parsed before body is consumed so that caller can use it
            write(request);
            return new FetchResult(request, response, exception);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private MessageDigest tryCalculatingPayloadDigest(FileChannel channel) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
        try {
            HttpResponse httpResponse = HttpResponse.parse(channel);
            byte[] buffer = new byte[8192];
            InputStream steam = httpResponse.body().stream();
            long payloadLength = 0;
            while (true) {
                int n = steam.read(buffer);
                if (n < 0) break;
                digest.update(buffer, 0, n);
                payloadLength += n;
            }
            if (payloadLength == 0) {
                return null;
            }
        } catch (Exception e) {
           return null;
        }
        return digest;
    }

    /**
     * Returns the byte position the next record will be written to.
     * <p>
     * If the underlying channel is not seekable the returned value will be relative to the position the channel
     * was in when the WarcWriter was created.
     */
    public long position() {
        return position.get();
    }

    /**
     * Closes the channel. If there are any active fetches they will be truncated and current progress written.
     */
    @Override
    public void close() throws IOException {
        closing = true;
        for (Socket socket: fetchSockets) {
            socket.close();
        }

        // block until all fetches have finished writing
        closeLock.writeLock().lock();
        try {
            channel.close();
        } finally {
            closeLock.writeLock().unlock();
        }
    }
}
