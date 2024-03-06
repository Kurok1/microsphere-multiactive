package io.microsphere.multiple.active.zone.jdbc.mysql;

/**
 * @author <a href="mailto:maimengzzz@gmail.com">韩超</a>
 * @since 1.0.0
 */
public class ZoneConnectionHostInfo {

    private String zone;
    private String hostPortPair;


    public static ZoneConnectionHostInfo of(String zone, String hostPortPair) {
        ZoneConnectionHostInfo hostInfo = new ZoneConnectionHostInfo();

        hostInfo.setZone(zone);
        hostInfo.setHostPortPair(hostPortPair);

        return hostInfo;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getHostPortPair() {
        return hostPortPair;
    }

    public void setHostPortPair(String hostPortPair) {
        this.hostPortPair = hostPortPair;
    }
}
