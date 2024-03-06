package io.microsphere.multiple.active.zone.jdbc.mysql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static org.junit.jupiter.api.Assertions.*;

class ZonePreferenceBalanceStrategyTest {

    private final String jdbcUrl = "jdbc:mysql:replication://localhost:33306,localhost:3306/test?ha.loadBalanceStrategy=io.microsphere.multiple.active.zone.jdbc.mysql.ZonePreferenceBalanceStrategy";

    @Test
    public void testConnection() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(jdbcUrl, "root", "123456");
        PreparedStatement statement = connection.prepareStatement("select 1");
        statement.execute();
    }

}