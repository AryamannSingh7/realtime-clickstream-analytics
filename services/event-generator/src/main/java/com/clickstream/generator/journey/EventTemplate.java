package com.clickstream.generator.journey;

import com.clickstream.avro.EventType;

/**
 * A fully-resolved, ready-to-emit step of a user journey. The simulator pre-computes
 * the variable parts (path, and any product/price/revenue) once when the journey is
 * built so the hot emit loop only has to stamp identity + time and send.
 *
 * @param type      the event type to emit
 * @param path      the page path for this step
 * @param productId product sku when the step concerns a product, else {@code null}
 * @param price     unit price when applicable, else {@code null}
 * @param revenue   order revenue for purchase steps, else {@code null}
 */
public record EventTemplate(
        EventType type,
        String path,
        String productId,
        Double price,
        Double revenue) {

    public static EventTemplate of(EventType type, String path) {
        return new EventTemplate(type, path, null, null, null);
    }

    public static EventTemplate ofProduct(EventType type, String path, String productId, Double price) {
        return new EventTemplate(type, path, productId, price, null);
    }

    public static EventTemplate ofPurchase(String path, String productId, Double price, Double revenue) {
        return new EventTemplate(EventType.purchase, path, productId, price, revenue);
    }
}
