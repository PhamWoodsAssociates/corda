package net.corda.node.services.network

import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.IdentityService
import net.corda.core.node.services.NetworkMapCache.MapChange
import net.corda.core.node.services.NotaryService
import net.corda.core.node.services.PartyInfo
import net.corda.core.schemas.NodeInfoSchemaV1
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.NetworkMapCacheBaseIntenal
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.utilities.CordaPersistence
import net.corda.node.utilities.bufferUntilDatabaseCommit
import net.corda.node.utilities.wrapWithDatabaseTransaction
import org.hibernate.Session
import rx.Observable
import rx.subjects.PublishSubject
import java.security.PublicKey
import java.util.*
import javax.annotation.concurrent.ThreadSafe
import kotlin.collections.HashMap

/**
 * Plumb thorough the updates coming from a [NetworkMapCacheBaseIntenal] to an [IdentityService].
 * Specifically add all the known nodes from the cache to the IdentityService and subscribe to the updates from the
 * cache and update the IdentityService accordingly.
 */
fun registerNetworkMapUpdatesInIdentityService(networkMapCacheIntenal: NetworkMapCacheBaseIntenal, identityService: IdentityService) {
    networkMapCacheIntenal.allNodes.forEach { it.legalIdentitiesAndCerts.forEach { identityService.verifyAndRegisterIdentity(it) } }
    networkMapCacheIntenal.changed.subscribe { mapChange ->
        // TODO how should we handle network map removal
        if (mapChange is MapChange.Added) {
            mapChange.node.legalIdentitiesAndCerts.forEach {
                identityService.verifyAndRegisterIdentity(it)
            }
        }
    }
}

/**
 * Extremely simple in-memory cache of the network map.
 */
@ThreadSafe
open class PersistentNetworkMapCacheBase(private val database: CordaPersistence, configuration: NodeConfiguration) : SingletonSerializeAsToken(), NetworkMapCacheBaseIntenal {
    companion object {
        val logger = loggerFor<PersistentNetworkMapCacheBase>()
    }

    // TODO Small explanation, partyNodes and registeredNodes is left in memory as it was before, because it will be removed in
    //  next PR that gets rid of services. These maps are used only for queries by service.
    protected val registeredNodes: MutableMap<PublicKey, NodeInfo> = Collections.synchronizedMap(HashMap())
    protected val partyNodes: MutableList<NodeInfo> get() = registeredNodes.map { it.value }.toMutableList()
    private val _changed = PublishSubject.create<MapChange>()
    // We use assignment here so that multiple subscribers share the same wrapped Observable.
    override val changed: Observable<MapChange> = _changed.wrapWithDatabaseTransaction()
    private val changePublisher: rx.Observer<MapChange> get() = _changed.bufferUntilDatabaseCommit()

    // TODO revisit the logic under which nodeReady and loadDBSuccess are set.
    // with the NetworkMapService redesign their meaning is not too well defined.
    private val _registrationFuture = openFuture<Void?>()
    override val nodeReady: CordaFuture<Void?> get() = _registrationFuture
    private var _loadDBSuccess: Boolean = false
    override val loadDBSuccess get() = _loadDBSuccess
    // TODO From the NetworkMapService redesign doc: Remove the concept of network services.
    //  As a temporary hack, just assume for now that every network has a notary service named "Notary Service" that can be looked up in the map.
    //  This should eliminate the only required usage of services.
    //  It is ensured on node startup when constructing a notary that the name contains "notary".
    override val notaryIdentities: List<Party>
        get() {
            return partyNodes
                    .flatMap {
                        // TODO: validate notary identity certificates before loading into network map cache.
                        //       Notary certificates have to be signed by the doorman directly
                        it.legalIdentities
                    }
                    .filter { it.name.commonName?.startsWith(NotaryService.ID_PREFIX) ?: false }
                    .toSet() // Distinct, because of distributed service nodes
                    .sortedBy { it.name.toString() }
        }

    private val nodeInfoSerializer = NodeInfoWatcher(configuration.baseDirectory,
            configuration.additionalNodeInfoPollingFrequencyMsec)

    init {
        loadFromFiles()
        database.transaction { loadFromDB(session) }
    }

    private fun loadFromFiles() {
        logger.info("Loading network map from files..")
        nodeInfoSerializer.nodeInfoUpdates().subscribe { node -> addNode(node) }
    }

    override fun getPartyInfo(party: Party): PartyInfo? {
        val nodes = database.transaction { queryByIdentityKey(session, party.owningKey) }
        if (nodes.size == 1 && nodes[0].isLegalIdentity(party)) {
            return PartyInfo.SingleNode(party, nodes[0].addresses)
        }
        for (node in nodes) {
            for (identity in node.legalIdentities) {
                if (identity == party) {
                    return PartyInfo.DistributedNode(party)
                }
            }
        }
        return null
    }

    override fun getNodeByLegalName(name: CordaX500Name): NodeInfo? = getNodesByLegalName(name).firstOrNull()
    override fun getNodesByLegalName(name: CordaX500Name): List<NodeInfo> = database.transaction { queryByLegalName(session, name) }
    override fun getNodesByLegalIdentityKey(identityKey: PublicKey): List<NodeInfo> =
            database.transaction { queryByIdentityKey(session, identityKey) }

    override fun getNodeByAddress(address: NetworkHostAndPort): NodeInfo? = database.transaction { queryByAddress(session, address) }

    override fun getPeerCertificateByLegalName(name: CordaX500Name): PartyAndCertificate? = database.transaction { queryIdentityByLegalName(session, name) }

    override fun track(): DataFeed<List<NodeInfo>, MapChange> {
        synchronized(_changed) {
            return DataFeed(partyNodes, _changed.bufferUntilSubscribed().wrapWithDatabaseTransaction())
        }
    }

    override fun addNode(node: NodeInfo) {
        logger.info("Adding node with info: $node")
        synchronized(_changed) {
            registeredNodes[node.legalIdentities.first().owningKey]?.let {
                if (it.serial > node.serial) {
                    logger.info("Discarding older nodeInfo for ${node.legalIdentities.first().name}")
                    return
                }
            }
            val previousNode = registeredNodes.put(node.legalIdentities.first().owningKey, node) // TODO hack... we left the first one as special one
            if (previousNode == null) {
                logger.info("No previous node found")
                database.transaction {
                    updateInfoDB(node)
                    changePublisher.onNext(MapChange.Added(node))
                }
            } else if (previousNode != node) {
                logger.info("Previous node was found as: $previousNode")
                database.transaction {
                    updateInfoDB(node)
                    changePublisher.onNext(MapChange.Modified(node, previousNode))
                }
            } else {
                logger.info("Previous node was identical to incoming one - doing nothing")
            }
        }
        _loadDBSuccess = true // This is used in AbstractNode to indicate that node is ready.
        _registrationFuture.set(null)
        logger.info("Done adding node with info: $node")
    }

    override fun removeNode(node: NodeInfo) {
        logger.info("Removing node with info: $node")
        synchronized(_changed) {
            registeredNodes.remove(node.legalIdentities.first().owningKey)
            database.transaction {
                removeInfoDB(session, node)
                changePublisher.onNext(MapChange.Removed(node))
            }
        }
        logger.info("Done removing node with info: $node")
    }

    override val allNodes: List<NodeInfo>
        get() = database.transaction {
            getAllInfos(session).map { it.toNodeInfo() }
        }

    // Changes related to NetworkMap redesign
    // TODO It will be properly merged into network map cache after services removal.

    private fun getAllInfos(session: Session): List<NodeInfoSchemaV1.PersistentNodeInfo> {
        val criteria = session.criteriaBuilder.createQuery(NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        criteria.select(criteria.from(NodeInfoSchemaV1.PersistentNodeInfo::class.java))
        return session.createQuery(criteria).resultList
    }

    /**
     * Load NetworkMap data from the database if present. Node can start without having NetworkMapService configured.
     */
    private fun loadFromDB(session: Session) {
        logger.info("Loading network map from database...")
        val result = getAllInfos(session)
        for (nodeInfo in result) {
            try {
                logger.info("Loaded node info: $nodeInfo")
                val node = nodeInfo.toNodeInfo()
                addNode(node)
            } catch (e: Exception) {
                logger.warn("Exception parsing network map from the database.", e)
            }
        }
    }

    private fun updateInfoDB(nodeInfo: NodeInfo) {
        // TODO Temporary workaround to force isolated transaction (otherwise it causes race conditions when processing
        //  network map registration on network map node)
        database.dataSource.connection.use {
            val session = database.entityManagerFactory.withOptions().connection(it.apply {
                transactionIsolation = 1
            }).openSession()
            session.use {
                val tx = session.beginTransaction()
                // TODO For now the main legal identity is left in NodeInfo, this should be set comparision/come up with index for NodeInfo?
                val info = findByIdentityKey(session, nodeInfo.legalIdentitiesAndCerts.first().owningKey)
                val nodeInfoEntry = generateMappedObject(nodeInfo)
                if (info.isNotEmpty()) {
                    nodeInfoEntry.id = info[0].id
                }
                session.merge(nodeInfoEntry)
                tx.commit()
            }
        }
    }

    private fun removeInfoDB(session: Session, nodeInfo: NodeInfo) {
        val info = findByIdentityKey(session, nodeInfo.legalIdentitiesAndCerts.first().owningKey).single()
        session.remove(info)
    }

    private fun findByIdentityKey(session: Session, identityKey: PublicKey): List<NodeInfoSchemaV1.PersistentNodeInfo> {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.owningKeyHash = :owningKeyHash",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("owningKeyHash", identityKey.toStringShort())
        return query.resultList
    }

    private fun queryByIdentityKey(session: Session, identityKey: PublicKey): List<NodeInfo> {
        val result = findByIdentityKey(session, identityKey)
        return result.map { it.toNodeInfo() }
    }

    private fun queryIdentityByLegalName(session: Session, name: CordaX500Name): PartyAndCertificate? {
        val query = session.createQuery(
                // We do the JOIN here to restrict results to those present in the network map
                "SELECT DISTINCT l FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.name = :name",
                NodeInfoSchemaV1.DBPartyAndCertificate::class.java)
        query.setParameter("name", name.toString())
        val candidates = query.resultList.map { it.toLegalIdentityAndCert() }
        // The map is restricted to holding a single identity for any X.500 name, so firstOrNull() is correct here.
        return candidates.firstOrNull()
    }

    private fun queryByLegalName(session: Session, name: CordaX500Name): List<NodeInfo> {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.name = :name",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("name", name.toString())
        val result = query.resultList
        return result.map { it.toNodeInfo() }
    }

    private fun queryByAddress(session: Session, hostAndPort: NetworkHostAndPort): NodeInfo? {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.addresses a WHERE a.pk.host = :host AND a.pk.port = :port",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("host", hostAndPort.host)
        query.setParameter("port", hostAndPort.port)
        val result = query.resultList
        return if (result.isEmpty()) null
        else result.map { it.toNodeInfo() }.singleOrNull() ?: throw IllegalStateException("More than one node with the same host and port")
    }

    /** Object Relational Mapping support. */
    private fun generateMappedObject(nodeInfo: NodeInfo): NodeInfoSchemaV1.PersistentNodeInfo {
        return NodeInfoSchemaV1.PersistentNodeInfo(
                id = 0,
                addresses = nodeInfo.addresses.map { NodeInfoSchemaV1.DBHostAndPort.fromHostAndPort(it) },
                // TODO Another ugly hack with special first identity...
                legalIdentitiesAndCerts = nodeInfo.legalIdentitiesAndCerts.mapIndexed { idx, elem ->
                    NodeInfoSchemaV1.DBPartyAndCertificate(elem, isMain = idx == 0)
                },
                platformVersion = nodeInfo.platformVersion,
                serial = nodeInfo.serial
        )
    }

    override fun clearNetworkMapCache() {
        database.transaction {
            val result = getAllInfos(session)
            for (nodeInfo in result) session.remove(nodeInfo)
        }
    }
}