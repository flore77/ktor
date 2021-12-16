/*
* Copyright 2014-2021 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
*/

package io.ktor.server.routing

import io.ktor.events.EventDefinition
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.util.*
import io.ktor.util.pipeline.*
import kotlin.native.concurrent.*

@InternalAPI
public val RoutingFailureStatusCode: AttributeKey<HttpStatusCode> = AttributeKey("RoutingFailureStatusCode")

/**
 * Root routing node for an [Application]
 * @param application is an instance of [Application] for this routing
 */
@OptIn(InternalAPI::class)
public class Routing(
    public val application: Application
) : Route(
    parent = null,
    selector = RootRouteSelector(application.environment.rootPath),
    application.environment.developmentMode,
    application.environment
) {
    private val tracers = mutableListOf<(RoutingResolveTrace) -> Unit>()

    /**
     * Register a route resolution trace function.
     * See https://ktor.io/servers/plugins/routing.html#tracing for details
     */
    public fun trace(block: (RoutingResolveTrace) -> Unit) {
        tracers.add(block)
    }

    public suspend fun interceptor(context: PipelineContext<Unit, ApplicationCall>) {
        val resolveContext = RoutingResolveContext(this, context.call, tracers)
        when (val resolveResult = resolveContext.resolve()) {
            is RoutingResolveResult.Success ->
                executeResult(context, resolveResult.route, resolveResult.parameters)
            is RoutingResolveResult.Failure ->
                context.call.attributes.put(RoutingFailureStatusCode, resolveResult.errorStatusCode)
        }
    }

    private suspend fun executeResult(
        context: PipelineContext<Unit, ApplicationCall>,
        route: Route,
        parameters: Parameters
    ) {
        val routingCallPipeline = route.buildPipeline()
        val receivePipeline = merge(
            context.call.request.pipeline,
            routingCallPipeline.receivePipeline
        ) { ApplicationReceivePipeline(developmentMode) }

        val responsePipeline = merge(
            context.call.response.pipeline,
            routingCallPipeline.sendPipeline
        ) { ApplicationSendPipeline(developmentMode) }

        val routingCall = RoutingApplicationCall(
            context.call,
            route,
            context.coroutineContext,
            receivePipeline,
            responsePipeline,
            parameters
        )
        application.environment.monitor.raise(RoutingCallStarted, routingCall)
        try {
            routingCallPipeline.execute(routingCall)
        } finally {
            application.environment.monitor.raise(RoutingCallFinished, routingCall)
        }
    }

    private inline fun <Subject : Any, Context : Any, P : Pipeline<Subject, Context>> merge(
        first: P,
        second: P,
        build: () -> P
    ): P {
        if (first.isEmpty) {
            return second
        }
        if (second.isEmpty) {
            return first
        }
        return build().apply {
            merge(first)
            merge(second)
        }
    }

    /**
     * Installable plugin for [Routing]
     */
    @Suppress("PublicApiImplicitType")
    public companion object Plugin : ApplicationPlugin<Application, Routing, Routing> {

        /**
         * Event definition for when a routing-based call processing starts
         */
        public val RoutingCallStarted: EventDefinition<RoutingApplicationCall> = EventDefinition()

        /**
         * Event definition for when a routing-based call processing finished
         */
        public val RoutingCallFinished: EventDefinition<RoutingApplicationCall> = EventDefinition()

        override val key: AttributeKey<Routing> = AttributeKey("Routing")

        override fun install(pipeline: Application, configure: Routing.() -> Unit): Routing {
            val routing = Routing(pipeline).apply(configure)
            pipeline.intercept(Call) { routing.interceptor(this) }
            return routing
        }
    }
}

/**
 * Gets an [Application] for this [Route] by scanning the hierarchy to the root
 */
public val Route.application: Application
    get() = when (this) {
        is Routing -> application
        else -> parent?.application ?: throw UnsupportedOperationException(
            "Cannot retrieve application from unattached routing entry"
        )
    }

/**
 * Gets or installs a [Routing] plugin for the this [Application] and runs a [configuration] script on it
 */
@ContextDsl
public fun Application.routing(configuration: Routing.() -> Unit): Routing =
    pluginOrNull(Routing)?.apply(configuration) ?: install(Routing, configuration)
