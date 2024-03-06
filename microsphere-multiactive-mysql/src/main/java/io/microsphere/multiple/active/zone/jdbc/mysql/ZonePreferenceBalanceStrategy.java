package io.microsphere.multiple.active.zone.jdbc.mysql;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.BalanceStrategy;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import io.microsphere.multiple.active.zone.ZoneContext;
import io.microsphere.multiple.active.zone.ZonePreferenceFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:maimengzzz@gmail.com">韩超</a>
 * @since 1.0.0
 */
public class ZonePreferenceBalanceStrategy implements BalanceStrategy {

    private final static Logger logger = LoggerFactory.getLogger(ZonePreferenceFilter.class);

    private final ZonePreferenceFilter<ZoneConnectionHostInfo> zonePreferenceFilter;
    private final ZoneConnectionHostInfoContainer container = ZoneConnectionHostInfoContainer.getContainer();

    public ZonePreferenceBalanceStrategy() {
        this.zonePreferenceFilter = new ZonePreferenceFilter<>(ZoneContext.get(), HostZoneResolver.INSTANCE);
    }


    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
        LoadBalancedConnectionProxy loadBalancedProxy = (LoadBalancedConnectionProxy) proxy;

        int numHosts = configuredHosts.size();

        SQLException ex = null;

        List<String> allowList = new ArrayList<>(numHosts);
        allowList.addAll(configuredHosts);

        Map<String, Long> blockList = loadBalancedProxy.getGlobalBlocklist();

        allowList.removeAll(blockList.keySet());

        if (allowList.size() == 0) {
            throw SQLError.createSQLException("no host found", null);
        }

        List<ZoneConnectionHostInfo> list = this.resolveHostInfo(allowList);
        //zone preference
        list = this.zonePreferenceFilter.filter(list);
        allowList = list.stream().map(ZoneConnectionHostInfo::getHostPortPair).collect(Collectors.toList());

        for (int attempts = 0; attempts < numRetries; ) {
            //deep copy
            Map<String, Integer> allowListMap = this.getArrayIndexMap(allowList);
            List<ZoneConnectionHostInfo> hostInfoList = new ArrayList<>(list);
            try {
                JdbcConnection connection = chooseOne(loadBalancedProxy, hostInfoList, liveConnections, allowListMap);

                if (connection == null) {
                    //all fail, retry
                    attempts++;
                    continue;
                }

                return connection;
            } catch (SQLException e) {
                //all fail, retry
                ex = e;
                attempts++;
            }
        }

        if (ex != null)
            throw ex;

        return null;
    }

    protected boolean shouldExceptionTriggerConnectionSwitch(SQLException sqlException) {
        return true;
    }


    public JdbcConnection chooseOne(LoadBalancedConnectionProxy loadBalancedProxy, List<ZoneConnectionHostInfo> allowList, Map<String, JdbcConnection> liveConnections, Map<String, Integer> allowListMap) throws SQLException {
        SQLException ex = null;
        String hostPortSpec = allowList.get(0).getHostPortPair();

        ConnectionImpl conn = (ConnectionImpl) liveConnections.get(hostPortSpec);

        while (true) {
            if (conn == null) {
                try {
                    conn = loadBalancedProxy.createConnectionForHost(hostPortSpec);
                } catch (SQLException sqlEx) {
                    ex = sqlEx;

                    if (shouldExceptionTriggerConnectionSwitch(sqlEx)) {

                        Integer allowListIndex = allowListMap.remove(hostPortSpec);

                        // exclude this host from being picked again
                        if (allowListIndex != null) {
                            allowList.remove(allowListIndex.intValue());
                        }
                        loadBalancedProxy.addToGlobalBlocklist(hostPortSpec);

                        if (allowList.size() != 0) {
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException e) {
                            }
                            continue;
                        }

                    }

                    throw sqlEx;
                }
            }

            return conn;
        }
    }

    private Map<String, Integer> getArrayIndexMap(List<String> l) {
        Map<String, Integer> m = new HashMap<>(l.size());
        for (int i = 0; i < l.size(); i++) {
            m.put(l.get(i), Integer.valueOf(i));
        }
        return m;

    }

    private List<ZoneConnectionHostInfo> resolveHostInfo(List<String> allowList) {
        List<ZoneConnectionHostInfo> list = new ArrayList<>(allowList.size());

        for (String host : allowList) {
            ZoneConnectionHostInfo hostInfo = this.container.getHostInfo(host);
            if (hostInfo != null)
                list.add(hostInfo);
        }

        return list;
    }
}
