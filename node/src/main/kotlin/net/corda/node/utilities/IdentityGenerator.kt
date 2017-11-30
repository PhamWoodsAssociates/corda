package net.corda.node.utilities

import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.cert
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.utilities.trace
import net.corda.nodeapi.internal.crypto.*
import org.slf4j.LoggerFactory
import java.nio.file.Path

object IdentityGenerator {
    private val log = LoggerFactory.getLogger(javaClass)

    const val NODE_IDENTITY_ALIAS_PREFIX = "identity"
    const val DISTRIBUTED_NOTARY_ALIAS_PREFIX = "distributed-notary"

    fun generateNodeIdentity(dir: Path, legalName: CordaX500Name): Party {
        return generateToDisk(listOf(dir), legalName, NODE_IDENTITY_ALIAS_PREFIX, threshold = 1)
    }

    fun generateDistributedNotaryIdentity(dirs: List<Path>, notaryName: CordaX500Name, threshold: Int = 1): Party {
        return generateToDisk(dirs, notaryName, DISTRIBUTED_NOTARY_ALIAS_PREFIX, threshold)
    }

    /**
     * Generates signing key pairs and a common distributed service identity for a set of nodes.
     * The key pairs and the group identity get serialized to disk in the corresponding node directories.
     * This method should be called *before* any of the nodes are started.
     *
     * @param dirs List of node directories to place the generated identity and key pairs in.
     * @param name The name of the identity.
     * @param threshold The threshold for the generated group [CompositeKey].
     */
    private fun generateToDisk(dirs: List<Path>,
                               name: CordaX500Name,
                               aliasPrefix: String,
                               threshold: Int): Party {
        log.trace { "Generating identity \"$name\" for nodes: ${dirs.joinToString()}" }
        val keyPairs = (1..dirs.size).map { generateKeyPair() }
        val key = CompositeKey.Builder().addKeys(keyPairs.map { it.public }).build(threshold)

        val caKeyStore = loadKeyStore(javaClass.classLoader.getResourceAsStream("net/corda/node/internal/certificates/cordadevcakeys.jks"), "cordacadevpass")
        val issuer = caKeyStore.getCertificateAndKeyPair(X509Utilities.CORDA_INTERMEDIATE_CA, "cordacadevkeypass")
        val rootCert = caKeyStore.getCertificate(X509Utilities.CORDA_ROOT_CA)

        keyPairs.zip(dirs) { keyPair, dir ->
            val serviceKeyCert = X509Utilities.createCertificate(CertificateType.CLIENT_CA, issuer.certificate, issuer.keyPair, name, keyPair.public)
            val compositeKeyCert = X509Utilities.createCertificate(CertificateType.CLIENT_CA, issuer.certificate, issuer.keyPair, name, key)
            val certPath = (dir / "certificates").createDirectories() / "distributedService.jks"
            val keystore = loadOrCreateKeyStore(certPath, "cordacadevpass")
            keystore.setCertificateEntry("$aliasPrefix-composite-key", compositeKeyCert.cert)
            keystore.setKeyEntry("$aliasPrefix-private-key", keyPair.private, "cordacadevkeypass".toCharArray(), arrayOf(serviceKeyCert.cert, issuer.certificate.cert, rootCert))
            keystore.save(certPath, "cordacadevpass")
        }

        return Party(name, key)
    }
}
