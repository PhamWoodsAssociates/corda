package net.corda.node.internal.protonwrapper.messages

import java.net.InetSocketAddress

interface ApplicationMessage {
    val payload: ByteArray
    val topic: String
    val destinationLegalName: String
    val destinationLink: InetSocketAddress
    val applicationProperties: Map<Any?, Any?>
}