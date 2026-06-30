package com.clickstream.generator.model;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Static catalog of e-commerce reference data — products, devices, geographies,
 * marketing campaigns and search terms — that the journey simulator draws from to
 * give events realistic, correlated attributes.
 *
 * <p>The data is intentionally small but plausible; it stays constant for the life
 * of the process so that downstream aggregations (top-N pages, funnels, revenue by
 * campaign) have a stable dimension space to roll up against.
 */
@Component
public class ReferenceData {

    /** A purchasable item. Price is fixed so revenue is reproducible per product. */
    public record Product(String id, String category, double price) {}

    /** A device fingerprint as it appears on the {@code device} sub-record. */
    public record DeviceProfile(String type, String os, String browser) {}

    /** A coarse location for the {@code geo} sub-record. */
    public record GeoLocation(String country, String city) {}

    /** A marketing campaign feeding {@code utm_source} / {@code utm_campaign}. */
    public record Campaign(String source, String campaign) {}

    private final List<Product> products;
    private final List<String> categories;
    private final List<DeviceProfile> devices;
    private final List<GeoLocation> geos;
    private final List<Campaign> campaigns;
    private final List<String> searchTerms;

    public ReferenceData() {
        this.categories = List.of(
                "electronics", "books", "home", "fashion", "sports", "toys", "grocery", "beauty");

        // ~40 products spread across the categories, prices anchored per category.
        this.products = buildProducts();

        this.devices = List.of(
                new DeviceProfile("desktop", "Windows", "Chrome"),
                new DeviceProfile("desktop", "macOS", "Safari"),
                new DeviceProfile("desktop", "Linux", "Firefox"),
                new DeviceProfile("mobile", "Android", "Chrome"),
                new DeviceProfile("mobile", "iOS", "Safari"),
                new DeviceProfile("tablet", "iPadOS", "Safari"),
                new DeviceProfile("tablet", "Android", "Chrome"));

        this.geos = List.of(
                new GeoLocation("US", "New York"),
                new GeoLocation("US", "San Francisco"),
                new GeoLocation("US", "Austin"),
                new GeoLocation("GB", "London"),
                new GeoLocation("DE", "Berlin"),
                new GeoLocation("FR", "Paris"),
                new GeoLocation("IN", "Bengaluru"),
                new GeoLocation("IN", "Mumbai"),
                new GeoLocation("JP", "Tokyo"),
                new GeoLocation("BR", "Sao Paulo"),
                new GeoLocation("CA", "Toronto"),
                new GeoLocation("AU", "Sydney"));

        this.campaigns = List.of(
                new Campaign("google", "summer_sale"),
                new Campaign("google", "brand_search"),
                new Campaign("facebook", "retargeting"),
                new Campaign("instagram", "influencer_q3"),
                new Campaign("email", "newsletter"),
                new Campaign("tiktok", "viral_launch"),
                new Campaign("bing", "shopping_feed"));

        this.searchTerms = List.of(
                "wireless headphones", "running shoes", "coffee maker", "yoga mat",
                "mechanical keyboard", "novel bestseller", "lego set", "face serum",
                "4k monitor", "standing desk", "protein powder", "board game");
    }

    private static List<Product> buildProducts() {
        // category -> base price; ids are sku-style and stable.
        return List.of(
                new Product("sku-1001", "electronics", 199.99),
                new Product("sku-1002", "electronics", 89.50),
                new Product("sku-1003", "electronics", 1299.00),
                new Product("sku-1004", "electronics", 349.99),
                new Product("sku-1005", "electronics", 59.99),
                new Product("sku-2001", "books", 14.99),
                new Product("sku-2002", "books", 24.99),
                new Product("sku-2003", "books", 9.99),
                new Product("sku-2004", "books", 39.99),
                new Product("sku-3001", "home", 49.99),
                new Product("sku-3002", "home", 129.00),
                new Product("sku-3003", "home", 19.99),
                new Product("sku-3004", "home", 299.00),
                new Product("sku-4001", "fashion", 79.99),
                new Product("sku-4002", "fashion", 34.99),
                new Product("sku-4003", "fashion", 149.99),
                new Product("sku-4004", "fashion", 22.50),
                new Product("sku-5001", "sports", 119.99),
                new Product("sku-5002", "sports", 44.99),
                new Product("sku-5003", "sports", 89.99),
                new Product("sku-5004", "sports", 12.99),
                new Product("sku-6001", "toys", 29.99),
                new Product("sku-6002", "toys", 59.99),
                new Product("sku-6003", "toys", 99.99),
                new Product("sku-7001", "grocery", 6.49),
                new Product("sku-7002", "grocery", 11.99),
                new Product("sku-7003", "grocery", 3.99),
                new Product("sku-8001", "beauty", 24.99),
                new Product("sku-8002", "beauty", 18.50),
                new Product("sku-8003", "beauty", 64.00));
    }

    public Product randomProduct() {
        return pick(products);
    }

    public String randomCategory() {
        return pick(categories);
    }

    public DeviceProfile randomDevice() {
        return pick(devices);
    }

    public GeoLocation randomGeo() {
        return pick(geos);
    }

    public Campaign randomCampaign() {
        return pick(campaigns);
    }

    public String randomSearchTerm() {
        return pick(searchTerms);
    }

    private static <T> T pick(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
