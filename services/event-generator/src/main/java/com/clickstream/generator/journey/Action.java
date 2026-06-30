package com.clickstream.generator.journey;

import com.clickstream.avro.EventType;

/**
 * A logical step a shopper can take in a session. Each action maps to the Avro
 * {@link EventType} that will be emitted; the concrete page {@code path} is filled
 * in per-event by the simulator because it often depends on the product/category/
 * search term chosen at that moment.
 */
public enum Action {

    LOGIN(EventType.login),
    LANDING(EventType.page_view),
    BROWSE(EventType.page_view),
    SEARCH(EventType.search),
    VIEW_PRODUCT(EventType.page_view),
    ADD_TO_CART(EventType.add_to_cart),
    REMOVE_FROM_CART(EventType.remove_from_cart),
    CHECKOUT(EventType.checkout_start),
    PURCHASE(EventType.purchase),
    LOGOUT(EventType.logout),
    /** Terminal marker — produces no event, just ends the journey. */
    EXIT(null);

    private final EventType eventType;

    Action(EventType eventType) {
        this.eventType = eventType;
    }

    public EventType eventType() {
        return eventType;
    }

    public boolean emitsEvent() {
        return eventType != null;
    }
}
