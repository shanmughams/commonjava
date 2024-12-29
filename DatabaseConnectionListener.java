package com.shan.spring.mssqldbcheck.config;

import com.shan.spring.mssqldbcheck.utils.ThreadUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
@Component
public class DatabaseConnectionListener implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnectionListener.class);

    static int MANUAL_BACKOFF = 2;

    @Autowired
    private DataSource dataSource;

    @Value("${spring.datasource.connection.validation.timeout:2}")
    private int connectionValidationTimeout;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Ensure the DataSource is an instance of HikariDataSource
        if (!(dataSource instanceof HikariDataSource)) {
            logger.error("DataSource is not an instance of HikariDataSource");
            System.exit(1);
        }

        try {
            // Attempt to check the database connection with manual retries
            checkDatabaseConnectionWithRetries();
            logger.info("Successfully established database connection");
        } catch (Exception e) {
            logger.error("Failed to establish database connection after retries", e);
            ThreadUtil.sleepInSecHandleInterrupt(1);
            logger.info("Exiting Application due to Failures");
            System.exit(1);
        }
    }

    private void checkDatabaseConnectionWithRetries() throws SQLException {
        HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
        int maxAttempts = 3; // Maximum number of attempts
        int attempt = 0; // Current attempt number

        while (attempt < maxAttempts) {
            try (Connection connection = hikariDataSource.getConnection()) {
                // If we successfully got a connection, validate it
                if (!connection.isValid(connectionValidationTimeout)) {
                    throw new SQLException("Database connection validation failed");
                }
                return; // Connection is valid, exit the method
            } catch (SQLException e) {
                attempt++;
                logger.warn("Failed to get a connection on attempt {}: {}", attempt, e.getMessage());

                if (attempt >= maxAttempts) {
                    throw e; // Re-throw exception if max attempts reached
                }

                // Manual backoff - wait before retrying
                try {
//                    Thread.sleep(2000); // Wait for 5 seconds before the next attempt
                    ThreadUtil.sleepInSec(attempt * MANUAL_BACKOFF);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // Restore interrupted status
                    throw new SQLException("Thread was interrupted during backoff", ie);
                }
            }
        }
    }
}
