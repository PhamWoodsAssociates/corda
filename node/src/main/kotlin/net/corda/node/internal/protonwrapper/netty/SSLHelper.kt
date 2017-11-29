package net.corda.node.internal.protonwrapper.netty

import io.netty.handler.ssl.SslHandler
import net.corda.node.internal.protonwrapper.netty.SSLHelper.ENABLED_SSL_CIPHER_SUITES
import net.corda.node.internal.protonwrapper.netty.SSLHelper.ENABLED_SSL_PROTOCOLS
import java.security.SecureRandom
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

object SSLHelper {
    val ENABLED_SSL_PROTOCOLS = arrayOf("TLSv1.2")
    val ENABLED_SSL_CIPHER_SUITES = arrayOf("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256")
}

fun createClientSslHelper(remoteHost: String,
                          remotePort: Int,
                          keyManagerFactory: KeyManagerFactory,
                          trustManagerFactory: TrustManagerFactory): SslHandler {
    val sslContext = SSLContext.getInstance("TLS")
    val keyManagers = keyManagerFactory.keyManagers
    val trustManagers = trustManagerFactory.trustManagers
    sslContext.init(keyManagers, trustManagers, SecureRandom())
    val sslEngine = sslContext.createSSLEngine(remoteHost, remotePort)
    sslEngine.useClientMode = true
    sslEngine.enabledProtocols = ENABLED_SSL_PROTOCOLS
    sslEngine.enabledCipherSuites = ENABLED_SSL_CIPHER_SUITES
    sslEngine.enableSessionCreation = true
    return SslHandler(sslEngine)
}

fun createServerSslHelper(keyManagerFactory: KeyManagerFactory,
                          trustManagerFactory: TrustManagerFactory): SslHandler {
    val sslContext = SSLContext.getInstance("TLS")
    val keyManagers = keyManagerFactory.keyManagers
    val trustManagers = trustManagerFactory.trustManagers
    sslContext.init(keyManagers, trustManagers, SecureRandom())
    val sslEngine = sslContext.createSSLEngine()
    sslEngine.useClientMode = false
    sslEngine.needClientAuth = true
    sslEngine.enabledProtocols = ENABLED_SSL_PROTOCOLS
    sslEngine.enabledCipherSuites = ENABLED_SSL_CIPHER_SUITES
    sslEngine.enableSessionCreation = true
    return SslHandler(sslEngine)
}