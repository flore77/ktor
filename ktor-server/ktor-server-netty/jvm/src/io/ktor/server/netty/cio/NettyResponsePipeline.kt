/*
* Copyright 2014-2021 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
*/

package io.ktor.server.netty.cio

import io.ktor.http.*
import io.ktor.server.netty.*
import io.ktor.server.netty.http2.*
import io.ktor.util.*
import io.ktor.util.cio.*
import io.ktor.utils.io.*
import io.netty.channel.*
import io.netty.handler.codec.http.*
import io.netty.handler.codec.http2.*
import kotlinx.coroutines.*
import java.io.*
import java.util.*
import kotlin.coroutines.*

private const val UNFLUSHED_LIMIT = 65536

@OptIn(InternalAPI::class)
internal class NettyResponsePipeline constructor(
    private val context: ChannelHandlerContext,
    initialEncapsulation: WriterEncapsulation,
    override val coroutineContext: CoroutineContext,
    private val responseQueue: Queue<NettyApplicationCall>
) : CoroutineScope {

    private var needsFlush: Boolean = false

    private var reading: Boolean = false

    private var encapsulation: WriterEncapsulation = initialEncapsulation

    fun markReadingStarted() {
        reading = true
    }

    fun markReadingStopped() {
        reading = false
        if (needsFlush) {
            needsFlush = false
            context.flush()
        }
    }

    fun processResponse(call: NettyApplicationCall) {
        // size is maximum -> exception
        responseQueue.add(call)

        //is size true?
        // cases when it's not (parallel exec?) or why we need return here
        if (responseQueue.size > 1) {
            return
        }

        startResponseProcessing()
    }

    private fun startResponseProcessing() {
        // can we add to the queue while iterating?
        while (true) {
            context.read()
            val call = responseQueue.poll() ?: break
            processElement(call)
        }
    }

    private fun processElement(call: NettyApplicationCall) {
        try {
            call.response.responseFlag.addListener {
                processCall(call)
            }
        } catch (actualException: Throwable) {
            processCallFailed(call, actualException)
        } finally {
            call.responseWriteJob.cancel()
        }
    }

    private fun processCallFailed(call: NettyApplicationCall, actualException: Throwable) {
        val t = when {
            actualException is IOException && actualException !is ChannelIOException ->
                ChannelWriteException(exception = actualException)
            else -> actualException
        }

        call.response.responseChannel.cancel(t)
        call.responseWriteJob.cancel()
        call.response.cancel()
        call.dispose()
    }

    private fun processUpgrade(responseMessage: Any): ChannelFuture {
        val future = context.write(responseMessage)
        encapsulation.upgrade(context)
        encapsulation = WriterEncapsulation.Raw
        needsFlush = true
        return future
    }

    private fun finishCall(call: NettyApplicationCall, lastMessage: Any?, lastFuture: ChannelFuture) {
        // what is isUpgradeResponse
        val prepareForClose = !call.request.keepAlive || call.response.isUpgradeResponse()

        val future = if (lastMessage != null) {
            context.write(lastMessage)
        } else {
            null
        }

        future?.addListener {
            if (prepareForClose) {
                close(lastFuture)
                return@addListener
            }
            if (responseQueue.isEmpty()) {
                // what is the difference between addListener and executor().execute
                scheduleFlush()
            }
        }

        if (prepareForClose) {
            close(lastFuture)
        }

        if (responseQueue.isEmpty()) {
            scheduleFlush()
        }
    }

    fun close(lastFuture: ChannelFuture) {
        context.flush()
        needsFlush = false
        lastFuture.addListener {
            context.close()
        }
    }

    private fun scheduleFlush() {
        context.executor().execute {
            if (responseQueue.isEmpty() && needsFlush) {
                needsFlush = false
                context.flush()
            }
        }
    }

    private fun processCall(call: NettyApplicationCall) {
        val responseMessage = call.response.responseMessage
        val response = call.response

        val requestMessageFuture = if (response.isUpgradeResponse()) {
            processUpgrade(responseMessage)
        } else {
            needsFlush = true
            context.write(responseMessage)
        }

        context.read()

        if (responseMessage is FullHttpResponse) {
            return finishCall(call, null, requestMessageFuture)
        } else if (responseMessage is Http2HeadersFrame && responseMessage.isEndStream) {
            return finishCall(call, null, requestMessageFuture)
        }

        val responseChannel = response.responseChannel
        val knownSize = when {
            responseChannel === ByteReadChannel.Empty -> 0
            responseMessage is HttpResponse -> responseMessage.headers().getInt("Content-Length", -1)
            responseMessage is Http2HeadersFrame -> responseMessage.headers().getInt("content-length", -1)
            else -> -1
        }

        // what context?
        launch(NettyDispatcher.CurrentContext(context)) {
            when (knownSize) {
                0 -> processEmpty(call, requestMessageFuture)
                in 1..65536 -> processSmallContent(call, response, knownSize)
                -1 -> processBodyFlusher(call, response, requestMessageFuture)
                else -> processBodyGeneral(call, response, requestMessageFuture)
            }
        }
    }

    private fun trailerMessage(response: NettyApplicationResponse): Any? {
        return if (response is NettyHttp2ApplicationResponse) {
            response.trailerMessage()
        } else {
            null
        }
    }

    private fun processEmpty(call: NettyApplicationCall, lastFuture: ChannelFuture) {
        return finishCall(call, encapsulation.endOfStream(false), lastFuture)
    }

    private suspend fun processSmallContent(call: NettyApplicationCall, response: NettyApplicationResponse, size: Int) {
        val buffer = context.alloc().buffer(size)
        val channel = response.responseChannel

        val start = buffer.writerIndex()
        channel.readFully(buffer.nioBuffer(start, buffer.writableBytes()))
        buffer.writerIndex(start + size)

        val future = context.write(encapsulation.transform(buffer, true))

        val lastMessage = trailerMessage(response) ?: encapsulation.endOfStream(true)
        finishCall(call, lastMessage, future)
    }

    private suspend fun processBodyGeneral(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        requestMessageFuture: ChannelFuture
    ) {
        val channel = response.responseChannel

        var unflushedBytes = 0
        var lastFuture: ChannelFuture = requestMessageFuture

        @Suppress("DEPRECATION")
        channel.lookAheadSuspend {
            while (true) {
                val buffer = request(0, 1)
                if (buffer == null) {
                    if (!awaitAtLeast(1)) break
                    continue
                }

                val rc = buffer.remaining()
                val buf = context.alloc().buffer(rc)
                val idx = buf.writerIndex()
                buf.setBytes(idx, buffer)
                buf.writerIndex(idx + rc)

                consumed(rc)
                unflushedBytes += rc

                val message = encapsulation.transform(buf, false)

                if (unflushedBytes >= UNFLUSHED_LIMIT) {
                    context.read()
                    val future = context.writeAndFlush(message)
                    lastFuture = future
                    future.suspendAwait()
                    unflushedBytes = 0
                } else {
                    lastFuture = context.write(message)
                }
            }
        }

        val lastMessage = trailerMessage(response) ?: encapsulation.endOfStream(false)
        finishCall(call, lastMessage, lastFuture)
    }

    private suspend fun processBodyFlusher(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        requestMessageFuture: ChannelFuture
    ) {
        val channel = response.responseChannel

        var unflushedBytes = 0
        var lastFuture: ChannelFuture = requestMessageFuture

        @Suppress("DEPRECATION")
        channel.lookAheadSuspend {
            while (true) {
                val buffer = request(0, 1)
                if (buffer == null) {
                    if (!awaitAtLeast(1)) break
                    continue
                }

                val rc = buffer.remaining()
                val buf = context.alloc().buffer(rc)
                val idx = buf.writerIndex()
                buf.setBytes(idx, buffer)
                buf.writerIndex(idx + rc)

                consumed(rc)
                unflushedBytes += rc

                val message = encapsulation.transform(buf, false)

                if (unflushedBytes >= UNFLUSHED_LIMIT || channel.availableForRead == 0) {
                    context.read()
                    val future = context.writeAndFlush(message)
                    lastFuture = future
                    future.suspendAwait()
                    unflushedBytes = 0
                } else {
                    lastFuture = context.write(message)
                }
            }
        }

        val lastMessage = trailerMessage(response) ?: encapsulation.endOfStream(false)
        finishCall(call, lastMessage, lastFuture)
    }
}

@OptIn(InternalAPI::class)
private fun NettyApplicationResponse.isUpgradeResponse() =
    status()?.value == HttpStatusCode.SwitchingProtocols.value
