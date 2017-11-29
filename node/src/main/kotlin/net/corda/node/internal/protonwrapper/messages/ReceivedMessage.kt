package net.corda.node.internal.protonwrapper.messages

import java.net.InetSocketAddress

interface ReceivedMessage : ApplicationMessage {
    val sourceLegalName: String
    val sourceLink: InetSocketAddress

    fun complete(accepted: Boolean)
}