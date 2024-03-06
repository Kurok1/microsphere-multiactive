package io.microsphere.multiple.active.zone.jdbc.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * the container for {@link ZoneConnectionHostInfo}
 * @author <a href="mailto:maimengzzz@gmail.com">韩超</a>
 * @since 1.0.0
 */
public class ZoneConnectionHostInfoContainer {

    private static final ZoneConnectionHostInfoContainer container = new ZoneConnectionHostInfoContainer();
    private static final Object lock = new Object();
    private static final String PREFIX = "microsphere.multiple.jdbc.";

    static {
        //load default configuration
        ClassLoader classLoader = ZoneConnectionHostInfoContainer.class.getClassLoader();
        try {
            Enumeration<URL> urls = classLoader.getResources("META-INF/microsphere-database-zone.properties");
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                container.addFromInputStream(url.openStream());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ZoneConnectionHostInfoContainer() {
        this(new ArrayList<>());
    }

    private ZoneConnectionHostInfoContainer(List<ZoneConnectionHostInfo> hostInfos) {
        this.hostInfos = hostInfos;
        for (ZoneConnectionHostInfo info : this.hostInfos)
            hostMap.put(info.getHostPortPair(), info);
    }


    private final List<ZoneConnectionHostInfo> hostInfos;
    private final Map<String, ZoneConnectionHostInfo> hostMap = new ConcurrentHashMap<>();

    public static ZoneConnectionHostInfoContainer getContainer() {
        if (container == null)
            throw new NullPointerException("container not be initialized");

        return container;
    }
    public List<ZoneConnectionHostInfo> getHostInfos() {
        return Collections.unmodifiableList(hostInfos);
    }

    public ZoneConnectionHostInfo getHostInfo(String hostPortPair) {
        return this.hostMap.get(hostPortPair);
    }

    private static void checkInitialized() {
        if (container != null) {
            throw new IllegalStateException("the container has been initialized");
        }
    }

    public ZoneConnectionHostInfoContainer addHost(ZoneConnectionHostInfo info) {
        synchronized (lock) {
            if (this.hostMap.containsKey(info.getHostPortPair()))
                return this;

            this.hostMap.put(info.getHostPortPair(), info);
            this.hostInfos.add(info);
            return this;
        }
    }

    public ZoneConnectionHostInfoContainer addHost(Collection<ZoneConnectionHostInfo> infos) {
        if (infos == null)
            throw new NullPointerException();
        synchronized (lock) {
            for (ZoneConnectionHostInfo info : infos) {
                addHost(info);
            }
            return this;
        }
    }

    public ZoneConnectionHostInfoContainer addFromInputStream(InputStream in) throws IOException {
        List<ZoneConnectionHostInfo> list = resolveInputStream(in);
        return addHost(list);
    }

    protected static List<ZoneConnectionHostInfo> resolveInputStream(InputStream in) throws IOException {
        Properties properties = new Properties();
        properties.load(in);
        List<ZoneConnectionHostInfo> list = new ArrayList<>();
        for (Object key : properties.keySet()) {
            String str = key.toString();
            if (str.startsWith(PREFIX)) {
                String zone = str.substring(PREFIX.length());
                String hostList = properties.getProperty(str, "");
                String[] hosts = hostList.split(",");
                for (String host : hosts) {
                    list.add(ZoneConnectionHostInfo.of(zone, host.trim()));
                }
            }
        }

        return list;
    }

}
