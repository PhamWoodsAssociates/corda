package net.corda.node.internal.protonwrapper.netty

import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import io.netty.util.internal.logging.InternalLoggerFactory
import io.netty.util.internal.logging.Slf4JLoggerFactory
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.protonwrapper.messages.ReceivedMessage
import net.corda.node.internal.protonwrapper.messages.SendableMessage
import net.corda.node.internal.protonwrapper.messages.impl.SendableMessageImpl
import rx.Observable
import rx.subjects.PublishSubject
import rx.subjects.Subject
import java.net.InetSocketAddress
import java.security.KeyStore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import kotlin.concurrent.withLock

class AMQPClient(val remoteHost: String,
                 val remotePort: Int,
                 keyStore: KeyStore,
                 keyStorePrivateKeyPassword: String,
                 trustStore: KeyStore) {
    companion object {
        init {
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
        }

        val log = loggerFor<AMQPClient>()
        const val RETRY_INTERVAL = 1000L
    }

    private val lock = ReentrantLock()
    @Volatile
    private var stopping: Boolean = false
    private var workerGroup: EventLoopGroup? = null
    @Volatile
    private var clientChannel: Channel? = null
    private val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
    private val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())

    init {
        keyManagerFactory.init(keyStore, keyStorePrivateKeyPassword.toCharArray())
        trustManagerFactory.init(trustStore)
    }

    private val connectListener = object : ChannelFutureListener {
        override fun operationComplete(future: ChannelFuture) {
            if (!future.isSuccess) {
                log.info("Failed to connect to $remoteHost $remotePort")

                if (!stopping) {
                    workerGroup?.schedule({
                        log.info("Retry connect to $remoteHost $remotePort")
                        restart()
                    }, RETRY_INTERVAL, TimeUnit.MILLISECONDS)
                }
            } else {
                log.info("Connected to $remoteHost $remotePort")
                // Connection established successfully
                clientChannel = future.channel()
                clientChannel?.closeFuture()?.addListener(closeListener)
            }
        }
    }

    private val closeListener = object : ChannelFutureListener {
        override fun operationComplete(future: ChannelFuture) {
            log.info("Disconnected from $remoteHost $remotePort")
            future.channel()?.disconnect()
            if (!stopping) {
                workerGroup?.schedule({
                    log.info("Retry connect to $remoteHost $remotePort")
                    restart()
                }, RETRY_INTERVAL, TimeUnit.MILLISECONDS)
            }
        }
    }

    private class ClientChannelInitializer(val remoteHost: String,
                                           val remotePort: Int,
                                           val keyManagerFactory: KeyManagerFactory,
                                           val trustManagerFactory: TrustManagerFactory,
                                           val onReceive: Subject<ReceivedMessage, ReceivedMessage>) : ChannelInitializer<SocketChannel>() {
        override fun initChannel(ch: SocketChannel) {
            val pipeline = ch.pipeline()
            val handler = createClientSslHelper(remoteHost, remotePort, keyManagerFactory, trustManagerFactory)
            pipeline.addLast("sslHandler", handler)
            pipeline.addLast("logger", LoggingHandler(LogLevel.INFO))
            pipeline.addLast(AMQPChannelHandler(false,
                    { rcv -> onReceive.onNext(rcv) }))
        }

    }

    fun start() {
        lock.withLock {
            log.info("connect to: $remoteHost $remotePort")
            workerGroup = NioEventLoopGroup()
            restart()
        }
    }

    private fun restart() {
        val bootstrap = Bootstrap()
        bootstrap.group(workerGroup).
                channel(NioSocketChannel::class.java).
                handler(ClientChannelInitializer(remoteHost, remotePort, keyManagerFactory, trustManagerFactory, _onReceive))
        val clientFuture = bootstrap.connect(remoteHost, remotePort)
        clientFuture.addListener(connectListener)
    }

    fun stop() {
        lock.withLock {
            log.info("disconnect from: $remoteHost $remotePort")
            stopping = true
            try {
                workerGroup?.shutdownGracefully()
                workerGroup?.terminationFuture()?.sync()
                workerGroup = null
            } finally {
                stopping = false
            }
            log.info("stopped connection to $remoteHost $remotePort")
        }
    }

    val connected: Boolean
        get() {
            val channel = lock.withLock { clientChannel }
            return channel?.isActive ?: false
        }

    fun createMessage(payload: ByteArray,
                      topic: String,
                      destinationLegalName: String,
                      destinationLink: InetSocketAddress,
                      properties: Map<Any?, Any?>): SendableMessage {
        require(destinationLink.hostName == remoteHost && destinationLink.port == remotePort) {
            "Message sent on incorrect link"
        }
        return SendableMessageImpl(payload, topic, destinationLegalName, destinationLink, properties)
    }

    fun write(msg: SendableMessage) {
        val channel = clientChannel
        channel?.apply {
            writeAndFlush(msg)
        }
    }

    private val _onReceive = PublishSubject.create<ReceivedMessage>().toSerialized()
    val onReceive: Observable<ReceivedMessage>
        get() = _onReceive
}