package com.clickstream.processor.topology;

import com.clickstream.avro.EventType;

/**
 * The four ordered stages of the conversion funnel. A visitor's {@code ordinal()} is their
 * progress: reaching a later stage implies every earlier one, which keeps the aggregate
 * counts monotonic ({@code VIEW >= CART >= CHECKOUT >= PURCHASE}).
 */
public enum FunnelStage {
    VIEW,
    CART,
    CHECKOUT,
    PURCHASE;

    /** Number of funnel stages (also the length of the counter array in the snapshot). */
    public static final int COUNT = values().length;

    /**
     * The funnel stage an event advances a visitor to, or {@code -1} for events that are not
     * funnel steps (click / search / remove_from_cart / login / logout).
     */
    public static int indexOf(EventType type) {
        return switch (type) {
            case page_view -> VIEW.ordinal();
            case add_to_cart -> CART.ordinal();
            case checkout_start -> CHECKOUT.ordinal();
            case purchase -> PURCHASE.ordinal();
            default -> -1;
        };
    }
}
