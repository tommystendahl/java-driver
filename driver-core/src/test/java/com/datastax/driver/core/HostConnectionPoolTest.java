package com.datastax.driver.core;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.fail;

public class HostConnectionPoolTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        StringBuilder sb = new StringBuilder("CREATE TABLE Java349 (mykey INT primary key");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i).append(" INT");
        }
        sb.append(")");
        return Lists.newArrayList(sb.toString());
    }

    /**
     * Ensures that if a fixed-sized pool has filled its core connections that borrowConnection will timeout instead
     * of creating a new connection.
     *
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void fixed_size_pool_should_fill_its_core_connections_and_then_timeout() throws ConnectionException, TimeoutException {
        HostConnectionPool pool = createPool(2, 2);

        assertThat(pool.connections.size()).isEqualTo(2);
        List<PooledConnection> coreConnections = Lists.newArrayList(pool.connections);

        for (int i = 0; i < 256; i++) {
            PooledConnection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).contains(connection);
        }

        boolean timedOut = false;
        try {
            pool.borrowConnection(100, MILLISECONDS);
        } catch (TimeoutException e) {
            timedOut = true;
        }
        assertThat(timedOut).isTrue();
    }

    /**
     * Ensures that if a variable-sized pool has filled up to its maximum connections that borrowConnection will
     * timeout instead of creating a new connection.
     *
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void variable_size_pool_should_fill_its_connections_and_then_timeout() throws ConnectionException, TimeoutException {
        HostConnectionPool pool = createPool(1, 2);

        assertThat(pool.connections.size()).isEqualTo(1);
        List<PooledConnection> coreConnections = Lists.newArrayList(pool.connections);

        //
        for (int i = 0; i < 128; i++) {
            PooledConnection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).contains(connection);
        }

        // Borrow more and ensure the connection returned is a non-core connection.
        for (int i = 0; i < 128; i++) {
            PooledConnection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).doesNotContain(connection);
        }

        boolean timedOut = false;
        try {
            pool.borrowConnection(100, MILLISECONDS);
        } catch (TimeoutException e) {
            timedOut = true;
        }
        assertThat(timedOut).isTrue();
    }

    /**
     * Ensures that if the core connection pool is full that borrowConnection will create and use a new connection.
     *
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_add_extra_connection_when_core_full() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20)
            .setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, 128);

        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        // Fill core connection
        for (int i = 0; i < 128; i++)
            assertThat(
                pool.borrowConnection(100, MILLISECONDS)
            ).isEqualTo(core);

        // Reaching 128 on the core connection should have triggered the creation of an extra one
        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        PooledConnection extra1 = pool.connections.get(1);

        assertThat(
            pool.borrowConnection(100, MILLISECONDS)
        ).isEqualTo(extra1);

        // If the extra connection is returned it gets trashed
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);

        // If the core connection gets returned we can borrow it again
        pool.returnConnection(core);
        assertThat(
            pool.borrowConnection(100, MILLISECONDS)
        ).isEqualTo(core);
    }

    /**
     * Ensures that a trashed connection that has not been timed out should be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @since 2.0.10, 2.1.5
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_resurrect_trashed_connection_within_idle_timeout() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20)
            .setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, 128);

        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        PooledConnection extra1 = pool.connections.get(1);

        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);

        // extra1 is now in the trash, core still full
        // Borrowing again should resurrect extra1 from the trash
        assertThat(
            pool.borrowConnection(100, MILLISECONDS)
        ).isEqualTo(extra1);
        assertThat(pool.connections).hasSize(2);
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @since 2.0.10, 2.1.5
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "long")
    public void should_not_resurrect_trashed_connection_after_idle_timeout() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20)
            .setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, 128);

        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        PooledConnection extra1 = pool.connections.get(1);

        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);

        // Give enough time for extra1 to be cleaned up from the trash:
        TimeUnit.SECONDS.sleep(30);

        // Next borrow should create another connection
        PooledConnection extra2 = pool.borrowConnection(100, MILLISECONDS);
        assertThat(extra2).isNotEqualTo(extra1);
        assertThat(extra1.isClosed()).isTrue();
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be closed until it has 0 in flight requests.
     *
     * @since 2.0.10, 2.1.5
     * @test_category connection:connection_pool
     */
    @Test(groups = "long")
    public void should_not_close_trashed_connection_until_no_in_flight()
            throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
                .setIdleTimeoutSeconds(20)
                .setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 128);

        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        PooledConnection extra1 = pool.connections.get(1);

        // Acquire twice and return 1, this will cause the connection to be put in the trash, but still have an
        // an in flight request.
        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);
        assertThat(extra1.isClosed()).isFalse();

        // Give enough time for extra1 to potentially be cleaned up from trash, but not be cleaned up since there
        // is still a request in flight.
        TimeUnit.SECONDS.sleep(30);
        assertThat(extra1.isClosed()).isFalse();

        // Return the connection, there should now be no requests in flight, thus the connection can be cleaned
        // from the trash and closed.
        pool.returnConnection(extra1);

        TimeUnit.SECONDS.sleep(30);

        // The connection should be now closed.
        assertThat(extra1.isClosed()).isTrue();
    }

    /**
     * Ensures that if a connection that has less than the minimum available stream ids is returned to the pool that
     * the connection is put in the trash.
     *
     * @since 2.0.10, 2.1.5
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_trash_on_returning_connection_with_insufficient_streams()
            throws ConnectionException, TimeoutException, InterruptedException {
        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);

        // Grab the new non-core connection and replace it with a spy.
        PooledConnection extra1 = spy(pool.connections.get(1));
        pool.connections.set(1, extra1);

        // Borrow 10 times to ensure pool is utilized.
        for (int i = 0; i < 10; i++) {
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        }

        // Return connection, it should not be trashed since it still has 9 inflight requests.
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(2);

        // stub the maxAvailableStreams method to return 0, indicating there are no remaining streams.
        // this should cause the connection to be replaced and trashed on returnConnection.
        doReturn(0).when(extra1).maxAvailableStreams();

        // On returning of the connection, should detect that there are no available streams and trash it.
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);
    }

    /**
     * Ensures that a trashed connection with insufficient stream ids that has not been timed out should not be
     * resurrected into the connection pool if borrowConnection is called and a new connection is needed.
     *
     * @since 2.0.10, 2.1.5
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_not_resurrect_connection_with_insufficient_streams()
            throws ConnectionException, TimeoutException, InterruptedException {
        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);

        // Grab the new non-core connection and replace it with a spy.
        PooledConnection extra1 = spy(pool.connections.get(1));
        pool.connections.set(1, extra1);

        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);

        // stub the maxAvailableStreams method to return 0, indicating there are no remaining streams.
        // this should cause the connection to not be revived from the trash.
        doReturn(0).when(extra1).maxAvailableStreams();

        // extra1 is now in the trash, core still full
        // Borrowing again should create a new connection.
        assertThat(pool.borrowConnection(100, MILLISECONDS)).isNotEqualTo(extra1);
        assertThat(pool.connections).hasSize(2);
    }

    /**
     * Ensures that trashed connections are closed when the connection pool they belonged to are closed.
     *
     * @since 2.0.10, 2.1.5
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_close_trashed_connections_when_closing_pool()
            throws ConnectionException, TimeoutException, InterruptedException, ExecutionException {
        HostConnectionPool pool = createPool(1, 2);
        PooledConnection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        PooledConnection extra1 = pool.connections.get(1);

        assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);

        // While the connection is trashed, it should not be closed.
        assertThat(extra1.isClosed()).isFalse();

        pool.closeAsync().get(1, TimeUnit.SECONDS);

        // Ensure both connections were closed.
        assertThat(core.isClosed()).isTrue();
        assertThat(extra1.isClosed()).isTrue();
    }

    private HostConnectionPool createPool(int coreConnections, int maxConnections) {
        cluster.getConfiguration().getPoolingOptions()
            .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnections)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        Session session = cluster.connect();
        Host host = TestUtils.findHost(cluster, 1);
        return ((SessionManager)session).pools.get(host);
    }

    /**
     * <p>
     * This test uses a table named "Java349" with 1000 column and performs asynchronously 100k insertions. While the
     * insertions are being executed, the number of opened connection is monitored.
     * <p/>
     * If at anytime, the number of opened connections is negative, this test will fail.
     *
     * @since 2.0.6, 2.1.1
     * @jira_ticket JAVA-349
     * @test_category connection:connection_pool
     */
    @Test(groups = "long", enabled = false /* this test causes timeouts on Jenkins */)
    public void open_connections_metric_should_always_be_positive() throws InterruptedException {
        // Track progress in a dedicated thread
        int numberOfInserts = 100 * 1000;
        final CountDownLatch pendingInserts = new CountDownLatch(numberOfInserts);
        ExecutorService progressReportExecutor = Executors.newSingleThreadExecutor();
        final Runnable progressReporter = new Runnable() {
            @Override
            public void run() {
                pendingInserts.countDown();
            }
        };

        // Track opened connections in a dedicated thread every one second
        final AtomicBoolean negativeOpenConnectionCountSpotted = new AtomicBoolean(false);
        final Gauge<Integer> openConnections = cluster.getMetrics().getOpenConnections();
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                Integer value = openConnections.getValue();
                if (value < 0) {
                    System.err.println("Negative value spotted for openConnection metric: " + value);
                    negativeOpenConnectionCountSpotted.set(true);
                }
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 1, 1, SECONDS);

        // Insert 100k lines in a newly created 1k columns table
        PreparedStatement insertStatement = session.prepare(generateJava349InsertStatement());
        for (int key = 0; key < numberOfInserts; key++) {
            session.executeAsync(insertStatement.bind(key)).addListener(progressReporter, progressReportExecutor);
        }

        // Wait for all inserts to happen and stop connections and progress tracking
        pendingInserts.await();
        openConnectionsWatcherExecutor.shutdownNow();
        progressReportExecutor.shutdownNow();

        if (negativeOpenConnectionCountSpotted.get()) {
            fail("Negative value spotted for open connection count");
        }
    }

    private String generateJava349InsertStatement() {
        StringBuilder sb = new StringBuilder("INSERT INTO Java349 (mykey");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i);
        }
        sb.append(") VALUES (?");
        for (int i = 0; i < 1000; i++) {
            sb.append(", ").append(i);
        }
        sb.append(");");
        return sb.toString();
    }
}
