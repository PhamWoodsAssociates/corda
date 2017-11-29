package net.corda.node.internal.protonwrapper

import net.corda.node.internal.protonwrapper.netty.AMQPServer
import net.corda.node.utilities.loadKeyStore
import java.nio.file.Paths

fun main(args: Array<String>) {
    println("Hello from server")

    val truststore = loadKeyStore(Paths.get("certificates/truststore.jks"), "trustpass")
    val keystore = loadKeyStore(Paths.get("certificates/sslkeystore.jks"), "cordacadevpass")
    val nettyServer = AMQPServer("localhost", 12345, keystore, "cordacadevpass", truststore)
    val subs = nettyServer.onReceive.subscribe { x ->
        println(">>> $x")
        x.complete(true)
    }
    nettyServer.start()
    readLine()
    println("Stopping")
    nettyServer.stop()
    subs.unsubscribe()
}