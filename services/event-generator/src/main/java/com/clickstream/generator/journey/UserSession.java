package com.clickstream.generator.journey;

import com.clickstream.generator.model.ReferenceData;
import java.util.List;

/**
 * One simulated shopper's visit: a stable identity (anonymous id, optional logged-in
 * user id, device, geo, acquisition channel) plus a pre-built {@link EventTemplate}
 * journey that is consumed one step at a time. When the journey is exhausted the
 * session is considered complete and the pool slot is recycled with a fresh visit.
 */
public final class UserSession {

    private final String anonymousId;
    private final String userId;          // null when the visit is anonymous
    private final ReferenceData.DeviceProfile device;
    private final ReferenceData.GeoLocation geo;
    private final String userAgent;
    private final String referrer;        // null for direct traffic
    private final String utmSource;       // null when not from a campaign
    private final String utmCampaign;     // null when not from a campaign
    private final List<EventTemplate> journey;

    private int index;

    public UserSession(
            String anonymousId,
            String userId,
            ReferenceData.DeviceProfile device,
            ReferenceData.GeoLocation geo,
            String userAgent,
            String referrer,
            String utmSource,
            String utmCampaign,
            List<EventTemplate> journey) {
        this.anonymousId = anonymousId;
        this.userId = userId;
        this.device = device;
        this.geo = geo;
        this.userAgent = userAgent;
        this.referrer = referrer;
        this.utmSource = utmSource;
        this.utmCampaign = utmCampaign;
        this.journey = journey;
        this.index = 0;
    }

    /** @return the next planned step, or {@code null} once the journey is finished. */
    public EventTemplate nextTemplate() {
        if (index >= journey.size()) {
            return null;
        }
        return journey.get(index++);
    }

    public boolean isComplete() {
        return index >= journey.size();
    }

    public String anonymousId() {
        return anonymousId;
    }

    public String userId() {
        return userId;
    }

    public ReferenceData.DeviceProfile device() {
        return device;
    }

    public ReferenceData.GeoLocation geo() {
        return geo;
    }

    public String userAgent() {
        return userAgent;
    }

    public String referrer() {
        return referrer;
    }

    public String utmSource() {
        return utmSource;
    }

    public String utmCampaign() {
        return utmCampaign;
    }
}
