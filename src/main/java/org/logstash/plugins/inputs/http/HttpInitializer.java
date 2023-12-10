package org.logstash.plugins.inputs.http;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;
import org.logstash.plugins.inputs.http.util.SslHandlerProvider;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import co.elastic.logstash.api.NamespacedMetric;

/**
 * Created by joaoduarte on 11/10/2017.
 */
public class HttpInitializer extends ChannelInitializer<SocketChannel> {
    private final IMessageHandler messageHandler;
    private SslHandlerProvider sslHandlerProvider;
    private final int maxContentLength;
    private final HttpResponseStatus responseStatus;
    private final ThreadPoolExecutor executorGroup;
    private final NamespacedMetric metric;
    private final AtomicInteger currentConnections = new AtomicInteger(0);
    private final AtomicInteger peakConnections = new AtomicInteger(0);
    public static final String CURRENT_CONNECTIONS = "current_connections";
    public static final String PEAK_CONNECTIONS = "peak_connections";

    public HttpInitializer(IMessageHandler messageHandler, ThreadPoolExecutor executorGroup,
                           int maxContentLength, HttpResponseStatus responseStatus, NamespacedMetric metric) {
        this.messageHandler = messageHandler;
        this.executorGroup = executorGroup;
        this.maxContentLength = maxContentLength;
        this.responseStatus = responseStatus;
        this.metric = metric;
    }

    protected void initChannel(SocketChannel socketChannel) throws Exception {
        synchronized (this) {
            int currentConnections = this.currentConnections.incrementAndGet();
            if (currentConnections > this.peakConnections.get()) {
                this.peakConnections.set(currentConnections);
                metric.gauge(PEAK_CONNECTIONS, currentConnections);
            }
            metric.gauge(CURRENT_CONNECTIONS, currentConnections);
        }

        ChannelPipeline pipeline = socketChannel.pipeline();

        if(sslHandlerProvider != null) {
            SslHandler sslHandler = sslHandlerProvider.getSslHandler(socketChannel.alloc());
            pipeline.addLast(sslHandler);
        }
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpContentDecompressor());
        pipeline.addLast(new HttpObjectAggregator(maxContentLength));
        pipeline.addLast(new HttpServerHandler(messageHandler.copy(), executorGroup, responseStatus, metric));

        socketChannel.closeFuture().addListener((ChannelFutureListener) future -> {
            synchronized (HttpInitializer.this) {
                metric.gauge(CURRENT_CONNECTIONS, this.currentConnections.decrementAndGet());
            }
        });
    }

    public void enableSSL(SslHandlerProvider sslHandlerProvider) {
        this.sslHandlerProvider = sslHandlerProvider;
    }
}

