package com.rohit.kafka.producer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Random;

public class MainVerticle extends AbstractVerticle {


    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        configureRouter(router);

        server.requestHandler(router::accept).listen(config().getInteger("http.port"));
    }

    private void configureRouter(Router router) {
        router.post("/messages")
                .produces("application/json")
                .handler(this::handlePostMessages)
                .failureHandler(this::handleFailures);


        router.post("/messages/latent/")
                .produces("application/json")
                .handler(this::handlePostMessageWithLatency)
                .failureHandler(this::handleFailures);
    }

    private void handlePostMessageWithLatency(RoutingContext routingContext) {
        int num = new Random(System.currentTimeMillis()).nextInt(100);
        boolean success = true;
        int latency = 0;
        if (num >= 50 && num < 80) {
            latency = 200;
        } else if (num >= 80 && num < 95) {
            latency = 2000;
        } else if (num >= 95) {
            success = false;
        }
        if (success) {
            if (latency > 0) {
                vertx.setTimer(latency, event -> {
                    if (!routingContext.response().closed()) {
                        routingContext.response().end();
                    }
                });
            } else {
                routingContext.response().end();
            }
        } else {
            routingContext.response().setStatusCode(400).end("Failed");
        }
    }

    private void handleFailures(RoutingContext routingContext) {
        routingContext.response().setStatusCode(500).end(routingContext.failure().getMessage());
    }

    private void handlePostMessages(RoutingContext routingContext) {
        routingContext.response().end("OK");
    }

}
