package net.corda.node.internal.protonwrapper.engine

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.protonwrapper.messages.MessageStatus
import net.corda.node.internal.protonwrapper.messages.impl.ReceivedMessageImpl
import net.corda.node.internal.protonwrapper.messages.impl.SendableMessageImpl
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.amqp.messaging.Accepted
import org.apache.qpid.proton.amqp.messaging.Rejected
import org.apache.qpid.proton.amqp.transport.DeliveryState
import org.apache.qpid.proton.amqp.transport.ErrorCondition
import org.apache.qpid.proton.engine.*
import org.apache.qpid.proton.engine.impl.CollectorImpl
import org.apache.qpid.proton.reactor.FlowController
import org.apache.qpid.proton.reactor.Handshaker
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class EventProcessor(channel: Channel, serverMode: Boolean, localLegalName: String, remoteLegalName: String) : BaseHandler() {
    companion object {
        private val log = loggerFor<EventProcessor>()
        const val FLOW_WINDOW_SIZE = 5
    }

    private val lock = ReentrantLock()
    private var pendingExecute: Boolean = false
    private val executor: ScheduledExecutorService = channel.eventLoop()
    private val collector = Proton.collector() as CollectorImpl
    private val handlers = mutableListOf<Handler>()
    private val tracker: ConnectionTracker = ConnectionTracker(serverMode, collector, localLegalName, remoteLegalName)

    val connection: Connection = tracker.connection

    init {
        addHandler(Handshaker())
        addHandler(FlowController(FLOW_WINDOW_SIZE))
        addHandler(tracker)
        connection.context = channel
        tick(tracker.connection)
    }

    fun addHandler(handler: Handler) = handlers.add(handler)

    private fun popEvent(): Event? {
        var ev = collector.peek()
        if (ev != null) {
            ev = ev.copy() // prevent mutation by collector.pop()
            collector.pop()
        }
        return ev
    }

    private fun tick(connection: Connection) {
        lock.withLock {
            try {
                if ((connection.localState != EndpointState.CLOSED) && !connection.transport.isClosed) {
                    val now = System.currentTimeMillis()
                    val tickDelay = Math.max(0L, connection.transport.tick(now) - now)
                    executor.schedule({ tick(connection) }, tickDelay, TimeUnit.MILLISECONDS)
                }
            } catch (ex: Exception) {
                connection.transport.close()
                connection.condition = ErrorCondition()
            }
        }
    }

    fun processEvents() {
        lock.withLock {
            pendingExecute = false
            log.info("Process Events")
            while (true) {
                val ev = popEvent() ?: break
                log.info("Process event: $ev")
                for (handler in handlers) {
                    handler.handle(ev)
                }
            }
            tracker.processTransport()
            log.info("Process Events Done")
        }
    }

    fun processEventsAsync() {
        lock.withLock {
            if (!pendingExecute) {
                pendingExecute = true
                executor.execute { processEvents() }
            }
        }
    }

    fun close() {
        if (connection.localState != EndpointState.CLOSED) {
            connection.close()
            processEvents()
            connection.free()
            processEvents()
        }
    }

    fun transportProcessInput(msg: ByteBuf) = lock.withLock { tracker.transportProcessInput(msg) }

    fun transportProcessOutput(ctx: ChannelHandlerContext) = lock.withLock { tracker.transportProcessOutput(ctx) }

    fun transportWriteMessage(msg: SendableMessageImpl) = lock.withLock { tracker.transportWriteMessage(msg) }

    fun complete(completer: ReceivedMessageImpl.MessageCompleter) = lock.withLock {
        val status: DeliveryState = if (completer.status == MessageStatus.Acknowledged) Accepted.getInstance() else Rejected()
        completer.delivery.disposition(status)
        completer.delivery.settle()
    }
}