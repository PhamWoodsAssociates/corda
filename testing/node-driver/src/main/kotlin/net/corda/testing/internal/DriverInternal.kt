package net.corda.testing.internal

import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import net.corda.core.concurrent.CordaFuture
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.copyTo
import net.corda.core.internal.div
import net.corda.core.internal.times
import net.corda.core.utilities.*
import net.corda.nodeapi.internal.addShutdownHook
import net.corda.testing.driver.DriverDSL
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.ListenProcessDeathException
import net.corda.testing.driver.PortAllocation
import net.corda.testing.initialiseTestSerialization
import net.corda.testing.node.NotarySpec
import java.net.Socket
import java.net.SocketException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

private val log = loggerFor<DriverDSL>()

interface InternalDriverDSL : DriverDSL {
    fun start()
    fun shutdown()
}

/**
 * This is a helper method to allow extending of the DSL, along the lines of
 *   interface SomeOtherExposedDSLInterface : DriverDSL
 *   interface SomeOtherInternalDSLInterface : InternalDriverDSL, SomeOtherExposedDSLInterface
 *   class SomeOtherDSL(val driverDSL : DriverDSLImpl) : InternalDriverDSL by driverDSL, SomeOtherInternalDSLInterface
 *
 * @param coerce We need this explicit coercion witness because we can't put an extra DI : D bound in a `where` clause.
 */
fun <DI : DriverDSL, D : InternalDriverDSL, A> genericDriver(
        driverDsl: D,
        initialiseSerialization: Boolean = true,
        coerce: (D) -> DI,
        dsl: DI.() -> A
): A {
    val serializationEnv = initialiseTestSerialization(initialiseSerialization)
    val shutdownHook = addShutdownHook(driverDsl::shutdown)
    try {
        driverDsl.start()
        return dsl(coerce(driverDsl))
    } catch (exception: Throwable) {
        log.error("Driver shutting down because of exception", exception)
        throw exception
    } finally {
        driverDsl.shutdown()
        shutdownHook.cancel()
        serializationEnv.resetTestSerialization()
    }
}

/**
 * This is a helper method to allow extending of the DSL, along the lines of
 *   interface SomeOtherExposedDSLInterface : DriverDSL
 *   interface SomeOtherInternalDSLInterface : InternalDriverDSL, SomeOtherExposedDSLInterface
 *   class SomeOtherDSL(val driverDSL : DriverDSLImpl) : InternalDriverDSL by driverDSL, SomeOtherInternalDSLInterface
 *
 * @param coerce We need this explicit coercion witness because we can't put an extra DI : D bound in a `where` clause.
 */
fun <DI : DriverDSL, D : InternalDriverDSL, A> genericDriver(
        defaultParameters: DriverParameters = DriverParameters(),
        isDebug: Boolean = defaultParameters.isDebug,
        driverDirectory: Path = defaultParameters.driverDirectory,
        portAllocation: PortAllocation = defaultParameters.portAllocation,
        debugPortAllocation: PortAllocation = defaultParameters.debugPortAllocation,
        systemProperties: Map<String, String> = defaultParameters.systemProperties,
        useTestClock: Boolean = defaultParameters.useTestClock,
        initialiseSerialization: Boolean = defaultParameters.initialiseSerialization,
        startNodesInProcess: Boolean = defaultParameters.startNodesInProcess,
        notarySpecs: List<NotarySpec>,
        extraCordappPackagesToScan: List<String> = defaultParameters.extraCordappPackagesToScan,
        driverDslWrapper: (DriverDSLImpl) -> D,
        coerce: (D) -> DI,
        dsl: DI.() -> A
): A {
    val serializationEnv = initialiseTestSerialization(initialiseSerialization)
    val driverDsl = driverDslWrapper(
            DriverDSLImpl(
                    portAllocation = portAllocation,
                    debugPortAllocation = debugPortAllocation,
                    systemProperties = systemProperties,
                    driverDirectory = driverDirectory.toAbsolutePath(),
                    useTestClock = useTestClock,
                    isDebug = isDebug,
                    startNodesInProcess = startNodesInProcess,
                    extraCordappPackagesToScan = extraCordappPackagesToScan,
                    notarySpecs = notarySpecs
            )
    )
    val shutdownHook = addShutdownHook(driverDsl::shutdown)
    try {
        driverDsl.start()
        return dsl(coerce(driverDsl))
    } catch (exception: Throwable) {
        log.error("Driver shutting down because of exception", exception)
        throw exception
    } finally {
        driverDsl.shutdown()
        shutdownHook.cancel()
        serializationEnv.resetTestSerialization()
    }
}

fun getTimestampAsDirectoryName(): String {
    return DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(Instant.now())
}

/**
 * @throws ListenProcessDeathException if [listenProcess] dies before the check succeeds, i.e. the check can't succeed as intended.
 */
fun addressMustBeBound(executorService: ScheduledExecutorService, hostAndPort: NetworkHostAndPort, listenProcess: Process? = null) {
    addressMustBeBoundFuture(executorService, hostAndPort, listenProcess).getOrThrow()
}

fun addressMustBeBoundFuture(executorService: ScheduledExecutorService, hostAndPort: NetworkHostAndPort, listenProcess: Process? = null): CordaFuture<Unit> {
    return poll(executorService, "address $hostAndPort to bind") {
        if (listenProcess != null && !listenProcess.isAlive) {
            throw ListenProcessDeathException(hostAndPort, listenProcess)
        }
        try {
            Socket(hostAndPort.host, hostAndPort.port).close()
            Unit
        } catch (_exception: SocketException) {
            null
        }
    }
}

/*
 * The default timeout value of 40 seconds have been chosen based on previous node shutdown time estimate.
 * It's been observed that nodes can take up to 30 seconds to shut down, so just to stay on the safe side the 60 seconds
 * timeout has been chosen.
 */
fun addressMustNotBeBound(executorService: ScheduledExecutorService, hostAndPort: NetworkHostAndPort, timeout: Duration = 40.seconds) {
    addressMustNotBeBoundFuture(executorService, hostAndPort).getOrThrow(timeout)
}

fun addressMustNotBeBoundFuture(executorService: ScheduledExecutorService, hostAndPort: NetworkHostAndPort): CordaFuture<Unit> {
    return poll(executorService, "address $hostAndPort to unbind") {
        try {
            Socket(hostAndPort.host, hostAndPort.port).close()
            null
        } catch (_exception: SocketException) {
            Unit
        }
    }
}

fun <A> poll(
        executorService: ScheduledExecutorService,
        pollName: String,
        pollInterval: Duration = 500.millis,
        warnCount: Int = 120,
        check: () -> A?
): CordaFuture<A> {
    val resultFuture = openFuture<A>()
    val task = object : Runnable {
        var counter = -1
        override fun run() {
            if (resultFuture.isCancelled) return // Give up, caller can no longer get the result.
            if (++counter == warnCount) {
                log.warn("Been polling $pollName for ${(pollInterval * warnCount.toLong()).seconds} seconds...")
            }
            try {
                val checkResult = check()
                if (checkResult != null) {
                    resultFuture.set(checkResult)
                } else {
                    executorService.schedule(this, pollInterval.toMillis(), TimeUnit.MILLISECONDS)
                }
            } catch (t: Throwable) {
                resultFuture.setException(t)
            }
        }
    }
    executorService.submit(task) // The check may be expensive, so always run it in the background even the first time.
    return resultFuture
}

fun writeConfig(path: Path, filename: String, config: Config) {
    val configString = config.root().render(ConfigRenderOptions.defaults())
    configString.byteInputStream().copyTo(path / filename, StandardCopyOption.REPLACE_EXISTING)
}
