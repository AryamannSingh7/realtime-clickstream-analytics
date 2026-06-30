package com.clickstream.generator.journey;

import com.clickstream.avro.EventType;
import com.clickstream.generator.model.ReferenceData;
import com.clickstream.generator.model.ReferenceData.Campaign;
import com.clickstream.generator.model.ReferenceData.DeviceProfile;
import com.clickstream.generator.model.ReferenceData.GeoLocation;
import com.clickstream.generator.model.ReferenceData.Product;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Builds realistic shopper journeys as a weighted walk through an e-commerce funnel.
 *
 * <p>Each call to {@link #newSession()} mints a fresh identity and pre-computes the
 * full sequence of events that visit will emit, so most sessions naturally drop off
 * before purchasing — giving the downstream funnel / conversion analytics meaningful
 * shape rather than a uniform distribution.
 */
@Component
public class JourneySimulator {

    /** Probability a visit is from a logged-in user. */
    private static final double LOGGED_IN_RATIO = 0.40;
    /** Probability a visit is attributed to a marketing campaign. */
    private static final double CAMPAIGN_RATIO = 0.50;
    /** Probability a logged-in visit ends with an explicit logout. */
    private static final double LOGOUT_RATIO = 0.30;
    /** Safety cap so a pathological walk can never loop forever. */
    private static final int MAX_STEPS = 40;

    private final ReferenceData ref;

    public JourneySimulator(ReferenceData ref) {
        this.ref = ref;
    }

    public UserSession newSession() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        String anonymousId = "anon-" + UUID.randomUUID();
        boolean loggedIn = rnd.nextDouble() < LOGGED_IN_RATIO;
        String userId = loggedIn ? "user-" + (100_000 + rnd.nextInt(900_000)) : null;

        DeviceProfile device = ref.randomDevice();
        GeoLocation geo = ref.randomGeo();
        String userAgent = userAgentFor(device);

        String utmSource = null;
        String utmCampaign = null;
        String referrer = "direct";
        if (rnd.nextDouble() < CAMPAIGN_RATIO) {
            Campaign c = ref.randomCampaign();
            utmSource = c.source();
            utmCampaign = c.campaign();
            referrer = "https://" + c.source() + ".com/";
        }

        List<EventTemplate> journey = buildJourney(loggedIn, rnd);
        return new UserSession(
                anonymousId, userId, device, geo, userAgent, referrer, utmSource, utmCampaign, journey);
    }

    private List<EventTemplate> buildJourney(boolean loggedIn, ThreadLocalRandom rnd) {
        List<EventTemplate> journey = new ArrayList<>();

        if (loggedIn) {
            journey.add(EventTemplate.of(EventType.login, "/login"));
        }

        Action state = Action.LANDING;
        Product currentProduct = null;

        for (int step = 0; step < MAX_STEPS && state != Action.EXIT; step++) {
            switch (state) {
                case LANDING -> journey.add(EventTemplate.of(EventType.page_view, "/"));
                case BROWSE -> journey.add(
                        EventTemplate.of(EventType.page_view, "/category/" + ref.randomCategory()));
                case SEARCH -> journey.add(
                        EventTemplate.of(EventType.search, "/search?q=" + encode(ref.randomSearchTerm())));
                case VIEW_PRODUCT -> {
                    currentProduct = ref.randomProduct();
                    journey.add(EventTemplate.ofProduct(
                            EventType.page_view, "/product/" + currentProduct.id(),
                            currentProduct.id(), currentProduct.price()));
                }
                case ADD_TO_CART -> {
                    Product p = ensureProduct(currentProduct);
                    currentProduct = p;
                    journey.add(EventTemplate.ofProduct(
                            EventType.add_to_cart, "/cart", p.id(), p.price()));
                }
                case REMOVE_FROM_CART -> {
                    Product p = ensureProduct(currentProduct);
                    journey.add(EventTemplate.ofProduct(
                            EventType.remove_from_cart, "/cart", p.id(), p.price()));
                }
                case CHECKOUT -> {
                    Product p = ensureProduct(currentProduct);
                    currentProduct = p;
                    journey.add(EventTemplate.ofProduct(
                            EventType.checkout_start, "/checkout", p.id(), p.price()));
                }
                case PURCHASE -> {
                    Product p = ensureProduct(currentProduct);
                    int qty = 1 + rnd.nextInt(3);
                    double revenue = round2(p.price() * qty);
                    journey.add(EventTemplate.ofPurchase(
                            "/checkout/complete", p.id(), p.price(), revenue));
                }
                default -> { /* LOGIN/LOGOUT/EXIT are not transitioned into here */ }
            }
            state = next(state, rnd);
        }

        if (loggedIn && rnd.nextDouble() < LOGOUT_RATIO) {
            journey.add(EventTemplate.of(EventType.logout, "/logout"));
        }
        return journey;
    }

    /** Weighted funnel transitions. Each branch sums to 1.0. */
    private Action next(Action state, ThreadLocalRandom rnd) {
        double r = rnd.nextDouble();
        return switch (state) {
            case LANDING -> r < 0.35 ? Action.BROWSE
                    : r < 0.65 ? Action.SEARCH
                    : r < 0.85 ? Action.VIEW_PRODUCT
                    : Action.EXIT;
            case BROWSE -> r < 0.45 ? Action.VIEW_PRODUCT
                    : r < 0.70 ? Action.BROWSE
                    : r < 0.85 ? Action.SEARCH
                    : Action.EXIT;
            case SEARCH -> r < 0.55 ? Action.VIEW_PRODUCT
                    : r < 0.75 ? Action.SEARCH
                    : r < 0.85 ? Action.BROWSE
                    : Action.EXIT;
            case VIEW_PRODUCT -> r < 0.35 ? Action.ADD_TO_CART
                    : r < 0.60 ? Action.VIEW_PRODUCT
                    : r < 0.75 ? Action.BROWSE
                    : r < 0.85 ? Action.SEARCH
                    : Action.EXIT;
            case ADD_TO_CART -> r < 0.45 ? Action.CHECKOUT
                    : r < 0.65 ? Action.VIEW_PRODUCT
                    : r < 0.75 ? Action.REMOVE_FROM_CART
                    : Action.EXIT;
            case REMOVE_FROM_CART -> r < 0.40 ? Action.VIEW_PRODUCT
                    : r < 0.65 ? Action.CHECKOUT
                    : Action.EXIT;
            case CHECKOUT -> r < 0.70 ? Action.PURCHASE : Action.EXIT;
            case PURCHASE -> Action.EXIT;
            default -> Action.EXIT;
        };
    }

    private Product ensureProduct(Product current) {
        return current != null ? current : ref.randomProduct();
    }

    private static String userAgentFor(DeviceProfile d) {
        return "Mozilla/5.0 (" + d.os() + "; " + d.type() + ") " + d.browser();
    }

    private static String encode(String term) {
        return term.replace(' ', '+');
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
