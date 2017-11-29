package net.corda.node.internal.protonwrapper

import com.google.common.util.concurrent.MoreExecutors
import net.corda.node.internal.protonwrapper.netty.AMQPClient
import net.corda.node.utilities.loadKeyStore
import java.net.InetSocketAddress
import java.nio.file.Paths
import java.util.*

fun main(args: Array<String>) {
    println("hello from client")
    val truststore = loadKeyStore(Paths.get("certificates/truststore.jks"), "trustpass")
    val keystore = loadKeyStore(Paths.get("certificates/sslkeystore.jks"), "cordacadevpass")
    val nettyClient = AMQPClient("localhost", 10005, keystore, "cordacadevpass", truststore)

    val subs = nettyClient.onReceive.subscribe { x -> println(">>> $x") }
    nettyClient.start()
    while (!nettyClient.connected) {
        Thread.sleep(100)
    }
    for (i in 0 until 1) {
        Thread.sleep(1000)
        val props = mapOf<Any?, Any?>("platform-topic" to "platform.session",
                "session-id" to 1234567890L,
                "corda-vendor" to "R3",
                "release-version" to "3.0-SNAPSHOT",
                "platform-version" to 2,
                "_AMQ_DUPL_ID" to UUID.randomUUID().toString())
        val msg = nettyClient.createMessage("A${i}A".toByteArray(),
                "p2p.inbound",
                "O=BankOfCorda,L=London,C=GB",
                InetSocketAddress.createUnresolved("localhost", 10005),
                props)
        msg.onComplete.addListener(Runnable { println(">>>> $msg ${msg.onComplete.get()}") }, MoreExecutors.directExecutor())
        nettyClient.write(msg)
    }
    readLine()
    nettyClient.stop()
    subs.unsubscribe()

}