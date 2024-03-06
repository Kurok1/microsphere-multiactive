package io.microsphere.multiple.active.zone.jdbc.mysql;

import io.microsphere.multiple.active.zone.ZoneResolver;

/**
 * resolve zone info from host
 * @author <a href="mailto:maimengzzz@gmail.com">韩超</a>
 * @since 1.0.0
 */
public class HostZoneResolver implements ZoneResolver<ZoneConnectionHostInfo> {

    public static final HostZoneResolver INSTANCE = new HostZoneResolver();

    private HostZoneResolver() {

    }

    @Override
    public String resolve(ZoneConnectionHostInfo entity) {
        return entity.getZone();
    }
}
