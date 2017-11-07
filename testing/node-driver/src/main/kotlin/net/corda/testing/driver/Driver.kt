@file:JvmName("Driver")

package net.corda.testing.driver

import net.corda.client.rpc.CordaRPCClient
import net.corda.cordform.CordformContext
import net.corda.cordform.CordformNode
import net.corda.core.CordaException
import net.corda.core.concurrent.CordaFuture
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.map
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.millis
import net.corda.node.internal.Node
import net.corda.node.internal.StartedNode
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.VerifierType
import net.corda.nodeapi.User
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.internal.DriverDSLImpl
import net.corda.testing.internal.genericDriver
import net.corda.testing.internal.getTimestampAsDirectoryName
import net.corda.testing.node.NotarySpec
import org.slf4j.Logger
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * This file defines a small "Driver" DSL for starting up nodes that is only intended for development, demos and tests.
 *
 * The process the driver is run in behaves as an Artemis client and starts up other processes.
 */
private val log: Logger = loggerFor<DriverDSLImpl>()

private val DEFAULT_POLL_INTERVAL = 500.millis

private const val DEFAULT_WARN_COUNT = 120

/**
 * Object ecapsulating a notary started automatically by the driver.
 */
data class NotaryHandle(val identity: Party, val validating: Boolean, val nodeHandles: CordaFuture<List<NodeHandle>>)

/**
 * This is the interface that's exposed to DSL users.
 */
interface DriverDSL : CordformContext {
    /** Returns a list of [NotaryHandle]s matching the list of [NotarySpec]s passed into [driver]. */
    val notaryHandles: List<NotaryHandle>

    /**
     * Returns the [NotaryHandle] for the single notary on the network. Throws if there are none or more than one.
     * @see notaryHandles
     */
    val defaultNotaryHandle: NotaryHandle get() {
        return when (notaryHandles.size) {
            0 -> throw IllegalStateException("There are no notaries defined on the network")
            1 -> notaryHandles[0]
            else -> throw IllegalStateException("There is more than one notary defined on the network")
        }
    }

    /**
     * Returns the identity of the single notary on the network. Throws if there are none or more than one.
     * @see defaultNotaryHandle
     */
    val defaultNotaryIdentity: Party get() = defaultNotaryHandle.identity

    /**
     * Returns a [CordaFuture] on the [NodeHandle] for the single-node notary on the network. Throws if there
     * are no notaries or more than one, or if the notary is a distributed cluster.
     * @see defaultNotaryHandle
     * @see notaryHandles
     */
    val defaultNotaryNode: CordaFuture<NodeHandle> get() {
        return defaultNotaryHandle.nodeHandles.map {
            it.singleOrNull() ?: throw IllegalStateException("Default notary is not a single node")
        }
    }

    /**
     * Start a node.
     *
     * @param defaultParameters The default parameters for the node. Allows the node to be configured in builder style
     *   when called from Java code.
     * @param providedName Optional name of the node, which will be its legal name in [Party]. Defaults to something
     *     random. Note that this must be unique as the driver uses it as a primary key!
     * @param verifierType The type of transaction verifier to use. See: [VerifierType]
     * @param rpcUsers List of users who are authorised to use the RPC system. Defaults to empty list.
     * @param startInSameProcess Determines if the node should be started inside the same process the Driver is running
     *     in. If null the Driver-level value will be used.
     * @return A [CordaFuture] on the [NodeHandle] to the node. The future will complete when the node is available.
     */
    fun startNode(
            defaultParameters: NodeParameters = NodeParameters(),
            providedName: CordaX500Name? = defaultParameters.providedName,
            rpcUsers: List<User> = defaultParameters.rpcUsers,
            verifierType: VerifierType = defaultParameters.verifierType,
            customOverrides: Map<String, Any?> = defaultParameters.customOverrides,
            startInSameProcess: Boolean? = defaultParameters.startInSameProcess,
            maximumHeapSize: String = defaultParameters.maximumHeapSize): CordaFuture<NodeHandle>

    /**
     * Helper function for starting a [Node] with custom parameters from Java.
     *
     * @param parameters The default parameters for the driver.
     * @return The value returned in the [dsl] closure.
     */
    fun startNode(parameters: NodeParameters): CordaFuture<NodeHandle> = startNode(defaultParameters = parameters)

    fun startNodes(
            nodes: List<CordformNode>,
            startInSameProcess: Boolean? = null,
            maximumHeapSize: String = "200m"
    ): List<CordaFuture<NodeHandle>>

    /** Call [startWebserver] with a default maximumHeapSize. */
    fun startWebserver(handle: NodeHandle): CordaFuture<WebserverHandle> = startWebserver(handle, "200m")

    /**
     * Starts a web server for a node
     * @param handle The handle for the node that this webserver connects to via RPC.
     * @param maximumHeapSize Argument for JVM -Xmx option e.g. "200m".
     */
    fun startWebserver(handle: NodeHandle, maximumHeapSize: String): CordaFuture<WebserverHandle>

    fun waitForAllNodesToFinish()

    /**
     * Polls a function until it returns a non-null value. Note that there is no timeout on the polling.
     *
     * @param pollName A description of what is being polled.
     * @param pollInterval The interval of polling.
     * @param warnCount The number of polls after the Driver gives a warning.
     * @param check The function being polled.
     * @return A future that completes with the non-null value [check] has returned.
     */
    fun <A> pollUntilNonNull(pollName: String,
                             pollInterval: Duration = DEFAULT_POLL_INTERVAL,
                             warnCount: Int = DEFAULT_WARN_COUNT,
                             check: () -> A?): CordaFuture<A>

    /**
     * Polls the given function until it returns true.
     * @see pollUntilNonNull
     */
    fun pollUntilTrue(pollName: String,
                      pollInterval: Duration = DEFAULT_POLL_INTERVAL,
                      warnCount: Int = DEFAULT_WARN_COUNT,
                      check: () -> Boolean): CordaFuture<Unit> {
        return pollUntilNonNull(pollName, pollInterval, warnCount) { if (check()) Unit else null }
    }

    val shutdownManager: ShutdownManager
}

sealed class NodeHandle {
    abstract val nodeInfo: NodeInfo
    /**
     * Interface to the node's RPC system. The first RPC user will be used to login if are any, otherwise a default one
     * will be added and that will be used.
     */
    abstract val rpc: CordaRPCOps
    abstract val configuration: NodeConfiguration
    abstract val webAddress: NetworkHostAndPort

    /**
     * Stops the referenced node.
     */
    abstract fun stop()

    data class OutOfProcess(
            override val nodeInfo: NodeInfo,
            override val rpc: CordaRPCOps,
            override val configuration: NodeConfiguration,
            override val webAddress: NetworkHostAndPort,
            val debugPort: Int?,
            val process: Process,
            private val onStopCallback: () -> Unit
    ) : NodeHandle() {
        override fun stop() {
            with(process) {
                destroy()
                waitFor()
            }
            onStopCallback()
        }
    }

    data class InProcess(
            override val nodeInfo: NodeInfo,
            override val rpc: CordaRPCOps,
            override val configuration: NodeConfiguration,
            override val webAddress: NetworkHostAndPort,
            val node: StartedNode<Node>,
            val nodeThread: Thread,
            private val onStopCallback: () -> Unit
    ) : NodeHandle() {
        override fun stop() {
            node.dispose()
            with(nodeThread) {
                interrupt()
                join()
            }
            onStopCallback()
        }
    }

    fun rpcClientToNode(): CordaRPCClient = CordaRPCClient(configuration.rpcAddress!!)
}

data class WebserverHandle(
        val listenAddress: NetworkHostAndPort,
        val process: Process
)

sealed class PortAllocation {
    abstract fun nextPort(): Int
    fun nextHostAndPort() = NetworkHostAndPort("localhost", nextPort())

    class Incremental(startingPort: Int) : PortAllocation() {
        val portCounter = AtomicInteger(startingPort)
        override fun nextPort() = portCounter.andIncrement
    }

    object RandomFree : PortAllocation() {
        override fun nextPort(): Int {
            return ServerSocket().use {
                it.bind(InetSocketAddress(0))
                it.localPort
            }
        }
    }
}

/** Helper builder for configuring a [Node] from Java. */
@Suppress("unused")
data class NodeParameters(
        val providedName: CordaX500Name? = null,
        val rpcUsers: List<User> = emptyList(),
        val verifierType: VerifierType = VerifierType.InMemory,
        val customOverrides: Map<String, Any?> = emptyMap(),
        val startInSameProcess: Boolean? = null,
        val maximumHeapSize: String = "200m"
) {
    fun setProvidedName(providedName: CordaX500Name?) = copy(providedName = providedName)
    fun setRpcUsers(rpcUsers: List<User>) = copy(rpcUsers = rpcUsers)
    fun setVerifierType(verifierType: VerifierType) = copy(verifierType = verifierType)
    fun setCustomerOverrides(customOverrides: Map<String, Any?>) = copy(customOverrides = customOverrides)
    fun setStartInSameProcess(startInSameProcess: Boolean?) = copy(startInSameProcess = startInSameProcess)
    fun setMaximumHeapSize(maximumHeapSize: String) = copy(maximumHeapSize = maximumHeapSize)
}

/**
 * [driver] allows one to start up nodes like this:
 *   driver {
 *     val noService = startNode(providedName = DUMMY_BANK_A.name)
 *     val notary = startNode(providedName = DUMMY_NOTARY.name)
 *
 *     (...)
 *   }
 *
 * Note that [DriverDSL.startNode] does not wait for the node to start up synchronously, but rather returns a [CordaFuture]
 * of the [NodeInfo] that may be waited on, which completes when the new node registered with the network map service or
 * loaded node data from database.
 *
 * @param defaultParameters The default parameters for the driver. Allows the driver to be configured in builder style
 *   when called from Java code.
 * @param isDebug Indicates whether the spawned nodes should start in jdwt debug mode and have debug level logging.
 * @param driverDirectory The base directory node directories go into, defaults to "build/<timestamp>/". The node
 *   directories themselves are "<baseDirectory>/<legalName>/", where legalName defaults to "<randomName>-<messagingPort>"
 *   and may be specified in [DriverDSL.startNode].
 * @param portAllocation The port allocation strategy to use for the messaging and the web server addresses. Defaults to incremental.
 * @param debugPortAllocation The port allocation strategy to use for jvm debugging. Defaults to incremental.
 * @param systemProperties A Map of extra system properties which will be given to each new node. Defaults to empty.
 * @param useTestClock If true the test clock will be used in Node.
 * @param startNodesInProcess Provides the default behaviour of whether new nodes should start inside this process or
 *     not. Note that this may be overridden in [DriverDSL.startNode].
 * @param notarySpecs The notaries advertised in the [NetworkParameters] for this network. These nodes will be started
 * automatically and will be available from [DriverDSL.notaryHandles]. Defaults to a simple validating notary.
 * @param dsl The dsl itself.
 * @return The value returned in the [dsl] closure.
 */
fun <A> driver(
        defaultParameters: DriverParameters = DriverParameters(),
        isDebug: Boolean = defaultParameters.isDebug,
        driverDirectory: Path = defaultParameters.driverDirectory,
        portAllocation: PortAllocation = defaultParameters.portAllocation,
        debugPortAllocation: PortAllocation = defaultParameters.debugPortAllocation,
        systemProperties: Map<String, String> = defaultParameters.systemProperties,
        useTestClock: Boolean = defaultParameters.useTestClock,
        initialiseSerialization: Boolean = defaultParameters.initialiseSerialization,
        startNodesInProcess: Boolean = defaultParameters.startNodesInProcess,
        notarySpecs: List<NotarySpec> = defaultParameters.notarySpecs,
        extraCordappPackagesToScan: List<String> = defaultParameters.extraCordappPackagesToScan,
        dsl: DriverDSL.() -> A
): A {
    return genericDriver(
            driverDsl = DriverDSLImpl(
                    portAllocation = portAllocation,
                    debugPortAllocation = debugPortAllocation,
                    systemProperties = systemProperties,
                    driverDirectory = driverDirectory.toAbsolutePath(),
                    useTestClock = useTestClock,
                    isDebug = isDebug,
                    startNodesInProcess = startNodesInProcess,
                    notarySpecs = notarySpecs,
                    extraCordappPackagesToScan = extraCordappPackagesToScan
            ),
            coerce = { it },
            dsl = dsl,
            initialiseSerialization = initialiseSerialization
    )
}

/**
 * Helper function for starting a [driver] with custom parameters from Java.
 *
 * @param parameters The default parameters for the driver.
 * @param dsl The dsl itself.
 * @return The value returned in the [dsl] closure.
 */
fun <A> driver(parameters: DriverParameters, dsl: DriverDSL.() -> A): A {
    return driver(defaultParameters = parameters, dsl = dsl)
}

/** Helper builder for configuring a [driver] from Java. */
@Suppress("unused")
data class DriverParameters(
        val isDebug: Boolean = false,
        val driverDirectory: Path = Paths.get("build", getTimestampAsDirectoryName()),
        val portAllocation: PortAllocation = PortAllocation.Incremental(10000),
        val debugPortAllocation: PortAllocation = PortAllocation.Incremental(5005),
        val systemProperties: Map<String, String> = emptyMap(),
        val useTestClock: Boolean = false,
        val initialiseSerialization: Boolean = true,
        val startNodesInProcess: Boolean = false,
        val notarySpecs: List<NotarySpec> = listOf(NotarySpec(DUMMY_NOTARY.name)),
        val extraCordappPackagesToScan: List<String> = emptyList()
) {
    fun setIsDebug(isDebug: Boolean) = copy(isDebug = isDebug)
    fun setDriverDirectory(driverDirectory: Path) = copy(driverDirectory = driverDirectory)
    fun setPortAllocation(portAllocation: PortAllocation) = copy(portAllocation = portAllocation)
    fun setDebugPortAllocation(debugPortAllocation: PortAllocation) = copy(debugPortAllocation = debugPortAllocation)
    fun setSystemProperties(systemProperties: Map<String, String>) = copy(systemProperties = systemProperties)
    fun setUseTestClock(useTestClock: Boolean) = copy(useTestClock = useTestClock)
    fun setInitialiseSerialization(initialiseSerialization: Boolean) = copy(initialiseSerialization = initialiseSerialization)
    fun setStartNodesInProcess(startNodesInProcess: Boolean) = copy(startNodesInProcess = startNodesInProcess)
    fun setExtraCordappPackagesToScan(extraCordappPackagesToScan: List<String>) = copy(extraCordappPackagesToScan = extraCordappPackagesToScan)
    fun setNotarySpecs(notarySpecs: List<NotarySpec>) = copy(notarySpecs = notarySpecs)
}

class ListenProcessDeathException(hostAndPort: NetworkHostAndPort, listenProcess: Process) :
        CordaException("The process that was expected to listen on $hostAndPort has died with status: ${listenProcess.exitValue()}")
