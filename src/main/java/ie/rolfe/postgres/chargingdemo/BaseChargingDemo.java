/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.rolfe.postgres.chargingdemo;

import com.google.gson.Gson;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 */
public abstract class BaseChargingDemo {

    public static final long GENERIC_QUERY_USER_ID = 42;
    public static final int HISTOGRAM_SIZE_MS = 1000000;
    public static final String CALL_ADD_CREDIT = "SELECT * FROM AddCredit(?,?,?)";
    public static final String CALL_REPORT_QUOTA_USAGE = "SELECT * FROM ReportQuotaUsage(?,?,?,?,?)";


    public static final String REPORT_QUOTA_USAGE = "ReportQuotaUsage";
    public static final String KV_PUT = "KV_PUT";
    public static final String KV_GET = "KV_GET";
    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";

    /**
     * Set to true to let calls for the same user run at the same time: each call goes to a
     * random worker, and users with a call in flight aren't skipped. Off by default, when
     * a user's calls always go to one worker and never overlap. Use it to see how the
     * database handles concurrent calls for one user (row lock conflicts).
     */
    public static final String SAME_USER_CONCURRENCY = "CHARGLT_SAME_USER_CONCURRENCY";

    /**
     * Optional lock_timeout, in milliseconds, for every connection. A call that waits longer
     * than this for a lock fails with SQLSTATE 55P03, as a NOWAIT call does at once. Not set
     * by default.
     */
    public static final String PG_LOCK_TIMEOUT_MS = "PG_LOCK_TIMEOUT_MS";

    /**
     * Optional override for ReferenceData.MAX_LOCK_RETRIES.
     */
    public static final String MAX_LOCK_RETRIES_ENV = "CHARGLT_MAX_LOCK_RETRIES";
    private static final int MAX_LOCK_RETRIES = getIntEnv(MAX_LOCK_RETRIES_ENV, ReferenceData.MAX_LOCK_RETRIES);

    public static final int BATCH_SIZE = 50;
    protected static final String QUEUE_QUEUE = "QUEUE_QUEUE";
    static final String ATTEMPTS = "ATTEMPTS";
    static final String QUEUE_LAG = "QUEUE_LAG";
    private static final String ADD_CREDIT = "ADD_CREDIT";

    /**
     * Counts calls that failed because the user's row was locked by another transaction
     */
    static final String ROW_LOCKED = "ROW_LOCKED";

    /**
     * Counts calls abandoned because the row was still locked after MAX_LOCK_RETRIES retries
     */
    static final String ROW_LOCKED_GAVE_UP = "ROW_LOCKED_GAVE_UP";

    /**
     * Histogram of attempts per call in executeWithLockRetry, including abandoned calls
     */
    static final String LOCK_ATTEMPTS = "LOCK_ATTEMPTS";

    /**
     * Histogram of the time (ms) calls spend in executeWithLockRetry: all attempts and the
     * sleeps between them, but not the client's queue. Includes abandoned calls.
     */
    static final String RETRY_LOOP = "RETRY_LOOP";

    /**
     * Histogram of the latency of abandoned calls, measured from the same point as ADD_CREDIT
     * and REPORT_QUOTA_USAGE, which leave them out.
     */
    static final String ABANDONED_CALL = "ABANDONED_CALL";
    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Connect to PostgreSQL using a hostname (or comma-delimited list for failover).
     * <p>
     * The connection is returned in autocommit mode. Every benchmark call site runs exactly
     * one statement per logical transaction, so letting the server commit it implicitly saves
     * a network round trip per transaction compared to an explicit commit() from the client.
     * Bulk loaders that want to batch many rows into one transaction should use
     * {@link #connectPG(String, String, String, boolean)} with autoCommit set to false.
     *
     * @param hostnames
     * @return
     * @throws Exception
     */
    protected static Connection connectPG(String hostnames, String appUser, String appPassword) throws Exception {
        return connectPG(hostnames, appUser, appPassword, true);
    }

    /**
     * Connect to PostgreSQL using a hostname (or comma-delimited list for failover).
     *
     * @param hostnames
     * @param autoCommit false if the caller wants to batch multiple statements per transaction
     *                   and commit them itself.
     * @return
     * @throws Exception
     */
    protected static Connection connectPG(String hostnames, String appUser, String appPassword,
                                          boolean autoCommit) throws Exception {
        String password = System.getenv("PG_PASSWORD");
        if (password == null) {
            password = appPassword;
        }
        String host = System.getenv("PG_HOST");
        if (host == null) {
            host = hostnames;
        }
        String user = System.getenv("PG_USER");
        if (user == null) {
            user = appUser;
        }

        String port = System.getenv("PG_PORT");
        if (port == null) {
            port = "5432";
        }

        String database = System.getenv("PG_DATABASE");
        if (database == null) {
            database = "charglt";
        }

        final String connectString = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        // Passed as properties rather than in the URL, so that passwords such as
        // Databricks Lakebase OAuth tokens don't need URL-encoding.
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        // Managed services such as Databricks Lakebase require "require".
        String sslMode = System.getenv("PG_SSLMODE");
        if (sslMode != null) {
            props.setProperty("sslmode", sslMode);
        }

        try {
            msg("Logging into PostgreSQL");

            Connection connection = DriverManager.getConnection(connectString, props);

            // Before setAutoCommit(false), so that a rollback can't undo it
            final int lockTimeoutMs = getIntEnv(PG_LOCK_TIMEOUT_MS, 0);
            if (lockTimeoutMs > 0) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("SET lock_timeout = " + lockTimeoutMs);
                }
            }

            connection.setAutoCommit(autoCommit);
            return connection;

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("DB connection failed.." + e.getMessage(), e);
        }
    }

    /**
     * Read an optional integer environment variable.
     *
     * @param name
     * @param defaultValue used if it isn't set or is empty
     */
    private static int getIntEnv(String name, int defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
    }

    /**
     * Commit, unless the connection is in autocommit mode and the server has already done it
     * for us. Lets the batching loaders share code with the autocommit benchmark paths.
     *
     * @param connection
     */
    protected static void commitIfNeeded(Connection connection) throws SQLException {

        if (!connection.getAutoCommit()) {
            connection.commit();
        }

    }

    /**
     * See if a call failed because another transaction has the user's row locked. Nothing
     * was changed, so the call can be repeated with the same transaction id.
     *
     * @param e
     * @return true if the error is lock_not_available
     */
    static boolean isRowLocked(SQLException e) {
        return ReferenceData.SQLSTATE_LOCK_NOT_AVAILABLE.equals(e.getSQLState());
    }

    /**
     * Execute a statement, retrying after a short random delay if the user's row is locked
     * by another transaction. The connection must be in autocommit mode: otherwise the
     * failed call has aborted a transaction that needs to be rolled back first. Reports
     * LOCK_ATTEMPTS and RETRY_LOOP once per call, whether it succeeds or is abandoned.
     *
     * @param statement
     * @return false if the row was still locked after MAX_LOCK_RETRIES retries
     * @throws SQLException any error other than the row being locked
     */
    static boolean executeWithLockRetry(PreparedStatement statement) throws SQLException {

        final long startNanos = System.nanoTime();

        for (int attempt = 0; ; attempt++) {
            try {
                statement.execute();
                reportRetryLoop(startNanos, attempt + 1);
                return true;
            } catch (SQLException e) {
                if (!isRowLocked(e)) {
                    throw e;
                }
            }

            shc.incCounter(ROW_LOCKED);

            if (attempt >= MAX_LOCK_RETRIES) {
                shc.incCounter(ROW_LOCKED_GAVE_UP);
                reportRetryLoop(startNanos, attempt + 1);
                return false;
            }

            sleepBeforeLockRetry(attempt);
        }
    }

    private static void reportRetryLoop(long startNanos, int attempts) {
        shc.report(LOCK_ATTEMPTS, attempts, "Attempts per call", MAX_LOCK_RETRIES + 2);
        shc.report(RETRY_LOOP, (int) ((System.nanoTime() - startNanos) / 1_000_000), "Time in retry loop", 10000);
    }

    /**
     * Sleep for a random time, which tends to be longer for later attempts.
     *
     * @param attempt 0 for the first retry
     */
    private static void sleepBeforeLockRetry(int attempt) {

        final int maxMs = Math.min(ReferenceData.LOCK_TIMEOUT_MS,
                ReferenceData.LOCK_RETRY_MS << Math.min(attempt, 10));

        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, maxMs + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Convenience method to generate a JSON payload.
     *
     * @param length
     * @param userCount - Each loyalty card will be used by 10 people for some reason...
     * @return
     */
    protected static String getExtraUserDataAsJsonString(int length, Gson gson, Random r, int userCount) {

        ExtraUserData eud = new ExtraUserData();

        eud.loyaltySchemeName = "HelperCard";
        eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r, userCount / 10);

        StringBuffer ourText = new StringBuffer();

        for (int i = 0; i < length / 2; i++) {
            ourText.append(Integer.toHexString(r.nextInt(256)));
        }

        eud.mysteriousHexPayload = ourText.toString();

        return gson.toJson(eud);
    }


    protected static void deleteAllUsers(int minUserId, int maxUserId, int tpMs,
                                         Connection mainConnection) throws InterruptedException {
        try {
            final long startMsUpsert = System.currentTimeMillis();

            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;
            Random r = new Random();

            PreparedStatement cs = mainConnection.prepareStatement("SELECT DelUser(?)");

            for (int i = minUserId; i <= maxUserId; i++) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                cs.setLong(1, i);
                cs.execute();

                if (i % 100000 == 1) {
                    msg("Deleted " + i + " users...");
                    commitIfNeeded(mainConnection);
                }

            }

            msg("All " + minUserId + " -> " + maxUserId + " entries in queue, waiting for it to drain...");
            commitIfNeeded(mainConnection);

            long entriesPerMS = (maxUserId - minUserId) / (System.currentTimeMillis() - startMsUpsert);
            msg("Deleted " + entriesPerMS + " users per ms...");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Create userCount users at tpMs per second.
     *
     * @param userCount
     * @param tpMs
     * @param gson
     * @param initialCredit
     * @param mainConnection
     */
    protected static void upsertAllUsers(int userCount, int tpMs, int loblength, Gson gson, int initialCredit,
                                         Connection mainConnection) throws InterruptedException {
        upsertUsers(0, 1, userCount, tpMs, loblength, gson, initialCredit, mainConnection);
    }

    /**
     * Create users firstUser, firstUser + step, firstUser + 2 * step ... up to userCount,
     * so that several threads, each with its own connection, can load disjoint users.
     *
     * @param firstUser
     * @param step
     * @param userCount
     * @param tpMs
     * @param gson
     * @param initialCredit
     * @param mainConnection
     */
    protected static void upsertUsers(int firstUser, int step, int userCount, int tpMs, int loblength, Gson gson,
                                      int initialCredit, Connection mainConnection) throws InterruptedException {
        try {
            final long startMsUpsert = System.currentTimeMillis();

            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;
            int upserted = 0;
            Random r = new Random();

            PreparedStatement cs = mainConnection.prepareStatement("SELECT * FROM UpsertUser(?,?,?,?,?,?)");

            // Upserts in the current database transaction, in case we have to redo it
            List<UserUpsert> uncommitted = new ArrayList<>();

            for (int i = firstUser; i < userCount; i += step) {

                String ourJson = getExtraUserDataAsJsonString(loblength, gson, r, userCount);

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                uncommitted.add(new UserUpsert(i, r.nextInt(initialCredit), ourJson,
                        new Timestamp(System.currentTimeMillis()), startMsUpsert + "_" + i));
                upsertLastOrRedoAll(uncommitted, cs, mainConnection);
                upserted++;

                if (mainConnection.getAutoCommit()) {
                    uncommitted.clear();
                }

                if (upserted % 100 == 1) {
                    msg("Upserted " + upserted + " users...");
                    commitIfNeeded(mainConnection);
                    uncommitted.clear();
                }

            }

            msg("All " + upserted + " entries in queue, waiting for it to drain...");
            commitIfNeeded(mainConnection);

            long elapsedMs = Math.max(1, System.currentTimeMillis() - startMsUpsert);
            msg("Upserted " + (upserted * 1000L / elapsedMs) + " users per second...");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Arguments for one call to UpsertUser.
     */
    private record UserUpsert(long userId, long balance, String json, Timestamp lastSeen, String txnId) {

        void execute(PreparedStatement cs) throws SQLException {
            cs.setLong(1, userId);
            cs.setLong(2, balance);
            cs.setString(3, json);
            cs.setString(4, "Initial Creation");
            cs.setTimestamp(5, lastSeen);
            cs.setString(6, txnId);
            cs.execute();
        }
    }

    /**
     * Call UpsertUser for the last entry in a list of uncommitted upserts. If that user's row
     * is locked by someone else the error aborts our whole database transaction, so we roll
     * it back, which also releases the rows we have locked, and redo everything in the list.
     *
     * @param uncommitted    upserts in the current database transaction; the last is new
     * @param cs             UpsertUser statement
     * @param mainConnection
     * @throws SQLException any error other than a locked row, or the row was still locked
     *                      after MAX_LOCK_RETRIES retries
     */
    private static void upsertLastOrRedoAll(List<UserUpsert> uncommitted, PreparedStatement cs,
                                            Connection mainConnection) throws SQLException {

        int firstToDo = uncommitted.size() - 1;

        for (int attempt = 0; ; attempt++) {
            try {
                for (UserUpsert upsert : uncommitted.subList(firstToDo, uncommitted.size())) {
                    upsert.execute(cs);
                }
                return;
            } catch (SQLException e) {
                if (!isRowLocked(e)) {
                    throw e;
                }

                shc.incCounter(ROW_LOCKED);

                if (!mainConnection.getAutoCommit()) {
                    mainConnection.rollback();
                    firstToDo = 0;
                }

                if (attempt >= MAX_LOCK_RETRIES) {
                    shc.incCounter(ROW_LOCKED_GAVE_UP);
                    throw e;
                }

                msg("Row locked by another session, redoing " + (uncommitted.size() - firstToDo)
                        + " upserts: " + e.getMessage());
            }

            sleepBeforeLockRetry(attempt);
        }
    }

    /**
     * Convenience method to query a user a general stats and log the results.
     *
     * @param mainConnection
     * @param queryUserId
     */
    protected static void queryUserAndStats(Connection mainConnection, long queryUserId) {


        // Query user #queryUserId...
        try {
            PreparedStatement cs = mainConnection.prepareStatement("SELECT * FROM GetUser(?)");
            cs.setLong(1, queryUserId);
            cs.execute();

            int resultSetCount = 0;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    resultSetCount++;
                    StringBuffer b = new StringBuffer("\nQuery :");
                    b.append(resultSetCount);

                    if (resultSet != null) {

                        while (resultSet.next()) {


                            for (int i = 1; i < resultSet.getMetaData().getColumnCount(); i++) {
                                b.append('\t');
                                b.append(resultSet.getString(i));
                            }


                        }
                        msg(b.toString());
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

//
//		msg("Show amount of credit currently reserved for products...");
//		ConnectionResponse allocResponse = mainConnection.callProcedure("ShowCurrentAllocations__promBL");
//
//		for (int i = 0; i < allocResponse.getResults().length; i++) {
//			msg(System.lineSeparator() + allocResponse.getResults()[i].toFormattedString());
//		}
    }

    /**
     *
     * Convenience method to query all users who have a specific loyalty card id
     *
     * @param mainConnection
     * @param cardId
     */
    protected static void queryLoyaltyCard(Connection mainConnection, long cardId) throws IOException {
        // Query user #queryUserId...
        msg("Query loyalty card #" + cardId + "...");
        try {
            PreparedStatement ps = mainConnection.prepareStatement("select * from user_table where user_json_cardid = ? order by userid;");
            ps.setLong(1, cardId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                msg(System.lineSeparator() + rs.getLong("userid") + "\t" + rs.getString("user_json_object"));
            }

        } catch (SQLException e) {
            msg("queryLoyaltyCard:" + e.getMessage());
        }


    }

    /**
     *
     * Run a key value store benchmark for userCount users at tpMs transactions per
     * millisecond and with deltaProportion records sending the entire record.
     *
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param globalQueryFreqSeconds
     * @param jsonsize
     * @param deltaProportion
     * @param extraMs
     * @return true if >=90% of requested throughput was achieved.
     * @throws InterruptedException
     */
    protected static boolean runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
                                            int jsonsize, Connection mainConnection, String hostlist, String username, String password, int deltaProportion, int extraMs, int workercount) throws InterruptedException {

        double tps = 0;

        try {
            long lastGlobalQueryMs = 0;

            UserKVState[] userState = new UserKVState[userCount];

            Random r = new Random();

            Gson gson = new Gson();

            for (int i = 0; i < userCount; i++) {
                userState[i] = new UserKVState(i, shc);
            }

            ChargingDemoKVWorker[] workers = new ChargingDemoKVWorker[workercount];
            Thread[] workerThreads = new Thread[workercount];

            msg("Creating " + workercount + " workers");
            for (int i = 0; i < workercount; i++) {
                workers[i] = new ChargingDemoKVWorker(i, hostlist, username, password, r, userState);
                Thread thread = new Thread(workers[i]);
                workerThreads[i] = thread;
                thread.start();
            }


            final long startMsRun = System.currentTimeMillis();
            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;

            final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

            // How many transactions we've done...
            int tranCount = 0;
            int inFlightCount = 0;

            PreparedStatement getAndLock = mainConnection.prepareStatement("SELECT * FROM GetAndLockUser(?,?)");
            PreparedStatement updateLockedUser = mainConnection.prepareStatement("SELECT * FROM UpdateLockedUser(?,?,?,?)");


            while (endtimeMs > System.currentTimeMillis()) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    sleepExtraMSIfNeeded(extraMs);

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                final long startMs = System.currentTimeMillis();

                // Find session to do a transaction for...
                int oursession = r.nextInt(userCount);

                // See if session already has an active transaction and avoid
                // it if it does.

                if (userState[oursession].isTxInFlight()) {

                    inFlightCount++;

                } else {

                    if (workercount == 0) {
                        doKVtransaction(userCount, jsonsize, deltaProportion, userState, oursession, getAndLock, startMs, r, updateLockedUser, gson);
                    } else {
                        KVRequest request = new KVRequest(userCount, jsonsize, deltaProportion, userState, oursession, startMs, r, gson);
                        workers[oursession % workercount].addRequest(request);
                    }


                }

                tranCount++;

                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }
            }

            // See if we need to do global queries...
            if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                lastGlobalQueryMs = System.currentTimeMillis();

                queryUserAndStats(mainConnection, GENERIC_QUERY_USER_ID);

            }

            msg(tranCount + " transactions done...");
            msg("All entries in queue, waiting for it to drain...");

            msg("Draining queue");
            for (int i = 0; i < workercount; i++) {
                workers[i].drain();
                workers[i].setKeepGoing(false);
            }
            //mainConnection.drain();
            msg("Queue drained...");

            long transactionsPerMs = tranCount / (System.currentTimeMillis() - startMsRun);
            msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");

            long lockFailCount = 0;
            for (int i = 0; i < userCount; i++) {
                lockFailCount += userState[i].getLockedBySomeoneElseCount();
            }

            msg(inFlightCount + " events where a tx was in flight were observed");
            msg(shc.getCounter("LOCK_COUNT") + " lock attempts");
            msg(shc.getCounter("CONTESTED_LOCK_COUNT") + " contested lock attempts");
            msg(lockFailCount + " lock attempt failures");
            msg(shc.getCounter("FULL_UPDATE") + " full updates");
            msg(shc.getCounter("DELTA_UPDATE") + " delta updates");

            tps = tranCount;
            tps = tps / (System.currentTimeMillis() - startMsRun);
            tps = tps * 1000;

            reportRunLatencyStats(tpMs, tps);


        } catch (SQLException e) {
            msg(e.getMessage());
        }
        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    static void doKVtransaction(int userCount, int jsonsize, int deltaProportion, UserKVState[] userState, int oursession, PreparedStatement getAndLock, long startMs, Random r, PreparedStatement updateLockedUser, Gson gson) throws SQLException {
        if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

            if (userState[oursession].getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                    .currentTimeMillis()) {

                userState[oursession].startTran();
                userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);

                getAndLock.setLong(1, oursession);
                getAndLock.setLong(2, oursession);
                getAndLock.execute();
                shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);

                shc.incCounter("LOCK_COUNT");

            } else {
                shc.incCounter("CONTESTED_LOCK_COUNT");
            }

        } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

            userState[oursession].startTran();
            userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
            getAndLock.setLong(1, oursession);
            getAndLock.setLong(2, oursession);
            getAndLock.execute();
            userState[oursession].setStatus(UserKVState.STATUS_LOCKED);
            shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);
            shc.incCounter("LOCK_COUNT");

        } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED) {

            userState[oursession].startTran();
            userState[oursession].setStatus(UserKVState.STATUS_UPDATING);

            if (deltaProportion > r.nextInt(101)) {

                // Instead of sending entire JSON object across wire ask app to update loyalty
                // number. For
                // large values stored as JSON this can have a dramatic effect on network
                // bandwidth
                updateLockedUser.setLong(1, oursession);
                updateLockedUser.setLong(2, oursession);
                updateLockedUser.setString(3, "" + getNewLoyaltyCardNumber(r, userCount / 10));
                updateLockedUser.setString(4, "NEW_LOYALTY_NUMBER");
                updateLockedUser.execute();
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
                shc.incCounter("DELTA_UPDATE");
            } else {

                // Instead of sending entire JSON object across wire ask app to update loyalty
                // number. For
                // large values stored as JSON this can have a dramatic effect on network
                // bandwidth
                updateLockedUser.setLong(1, oursession);
                updateLockedUser.setLong(2, oursession);
                updateLockedUser.setString(3, getExtraUserDataAsJsonString(jsonsize, gson, r, userCount));
                updateLockedUser.setString(4, "FULL_UPDATE");
                updateLockedUser.execute();
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
                shc.incCounter("FULL_UPDATE");
            }

            userState[oursession].setStatus(UserKVState.STATUS_UNLOCKED);

        }
    }

    /**
     * Used when we need to really slow down below 1 tx per ms..
     *
     * @param extraMs an arbitrary extra delay.
     */
    private static void sleepExtraMSIfNeeded(int extraMs) {
        if (extraMs > 0) {
            try {
                Thread.sleep(extraMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Convenience method to remove unneeded records storing old allotments of
     * credit.
     *
     * @param mainConnection
     * @throws IOException
     * @throws SQLException
     */
    protected static void clearUnfinishedTransactions(Connection mainConnection) throws IOException, SQLException {

        msg("Clearing unfinished transactions from prior runs...");

        java.sql.Statement stmt = mainConnection.createStatement();

        stmt.execute("TRUNCATE TABLE user_usage_table");
        commitIfNeeded(mainConnection);
        stmt.close();

        unlockAllRecords(mainConnection);

        msg("clearUnfinishedTransactions...done");

    }

    /**
     *
     * Convenience method to clear outstaning locks between runs
     *
     * @param mainConnection
     * @throws IOException
     */
    protected static void unlockAllRecords(Connection mainConnection) throws IOException, SQLException {

        msg("Clearing locked sessions from prior runs...");

        java.sql.Statement stmt = mainConnection.createStatement();

        stmt.execute(
                "UPDATE user_table SET user_softlock_sessionid = null, user_softlock_expiry = null WHERE user_softlock_sessionid IS NOT NULL");
        commitIfNeeded(mainConnection);
        msg("unlockAllRecords...done");

    }

    /**
     *
     * Run a transaction benchmark for userCount users at tpMs per ms.
     *
     * @param userCount              number of users
     * @param tpMs                   transactions per milliseconds
     * @param durationSeconds
     * @param globalQueryFreqSeconds how often we check on global stats and a single
     *                               user
     * @param mainConnection
     * @param workercount
     * @return true if within 90% of targeted TPS
     */
    protected static boolean runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
                                                     int globalQueryFreqSeconds, Connection mainConnection, int extraMs,
                                                     String hostlist, String username, String password, int workercount) throws InterruptedException {

        // Used to track changes and be unique when we are running multiple threads
        final long pid = getPid();

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[userCount];


        ChargingDemoTransactionWorker[] workers = new ChargingDemoTransactionWorker[workercount];
        Thread[] workerThreads = new Thread[workercount];

        msg("Creating " + workercount + " workers");
        for (int i = 0; i < workercount; i++) {
            workers[i] = new ChargingDemoTransactionWorker(i, hostlist, username, password, r, users);
            Thread thread = new Thread(workers[i]);
            workerThreads[i] = thread;
            thread.start();
        }


        msg("Creating Connection records for " + users.length + " users");
        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i, Long.MAX_VALUE);
        }

        final boolean sameUserConcurrency = Boolean.parseBoolean(System.getenv(SAME_USER_CONCURRENCY));
        if (sameUserConcurrency) {
            msg(SAME_USER_CONCURRENCY + " is true: calls for the same user may run at the same time");
        }

        final long startMsRun = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        long tranCount = 0;
        long inFlightCount = 0;
        long sentCount = 0;
        long lastGlobalQueryMs = System.currentTimeMillis();

        msg("starting...");

        PreparedStatement addCredit = null;
        PreparedStatement reportUsage = null;

        try {
            addCredit = mainConnection.prepareStatement(CALL_ADD_CREDIT);
            reportUsage = mainConnection.prepareStatement(CALL_REPORT_QUOTA_USAGE);

            while (endtimeMs > System.currentTimeMillis()) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    sleepExtraMSIfNeeded(extraMs);

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                int randomuser = r.nextInt(userCount);

                final long startMs = System.currentTimeMillis();

                if (!sameUserConcurrency && users[randomuser].isTxInFlight()) {
                    inFlightCount++;
                } else {

                    users[randomuser].startTran();
                    sentCount++;

                    if (workercount == 0) {
                        doTransaction(users, randomuser, r, addCredit, pid, startMs, reportUsage, tranCount);
                    } else {
                        TxRequest request = new TxRequest(tranCount, pid, randomuser);
                        int worker = sameUserConcurrency ? r.nextInt(workercount) : randomuser % workercount;
                        workers[worker].addRequest(request);
                    }

                }

                tranCount++;


                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }

                // See if we need to do global queries...
                if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                    lastGlobalQueryMs = System.currentTimeMillis();

                    queryUserAndStats(mainConnection, GENERIC_QUERY_USER_ID);
                    queryLoyaltyCard(mainConnection, GENERIC_QUERY_USER_ID);

                }

            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        msg("finished adding transactions to queue");

        msg("Draining queue");
        for (int i = 0; i < workercount; i++) {
            workers[i].drain();
            workers[i].setKeepGoing(false);
        }

        // Calls already taken off the queue may still be retrying, so wait for them before
        // we report any numbers
        for (Thread workerThread : workerThreads) {
            workerThread.join();
        }

        //mainConnection.drain();
        msg("Queue drained");

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + sentCount + " transactions in " + elapsedTimeMs + " milliseconds ("
                + inFlightCount + " more skipped because the user had one in flight)");

        // Count only the calls that the database carried out: not the skipped ones, nor those
        // we gave up on because the user's row stayed locked.
        final long gaveUpCount = shc.getCounter(ROW_LOCKED_GAVE_UP);
        double tps = sentCount - gaveUpCount;
        tps = tps / (elapsedTimeMs / 1000);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + shc.getCounter("ADD_CREDIT"));
        msg("Report Usage calls = " + shc.getCounter("REPORT_USAGE"));
        msg("Skipped because transaction was in flight = " + inFlightCount);
        msg("Attempts that failed because the user's row was locked = " + shc.getCounter(ROW_LOCKED));
        msg("Calls abandoned because the user's row stayed locked = " + gaveUpCount);

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    static void doTransaction(UserTransactionState[] users, int randomuser, Random r, PreparedStatement addCredit
            , long pid, long startMs, PreparedStatement reportUsage, long txId) throws SQLException {
        if (users[randomuser].spendableBalance < 1000) {

            shc.incCounter("ADD_CREDIT");

            final long extraCredit = r.nextInt(1000) + 1000;

            addCredit.setLong(1, randomuser);
            addCredit.setLong(2, extraCredit);
            addCredit.setString(3, "AddCreditOnShortage_" + pid + "_" + txId + "_" + System.currentTimeMillis());

            // If we give up the user's balance and reservation are unchanged
            if (executeWithLockRetry(addCredit)) {
                updateUserBalance(users[randomuser], addCredit, users[randomuser].sessionId);
                shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);
            } else {
                shc.reportLatency(ABANDONED_CALL, startMs, "Latency of abandoned call", 10000);
            }

        } else {

            shc.incCounter("REPORT_USAGE");


            long unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
            long unitsWanted = r.nextInt(100);
            reportUsage.setLong(1, randomuser);
            reportUsage.setLong(2, unitsUsed);
            reportUsage.setLong(3, unitsWanted);
            reportUsage.setLong(4, randomuser);
            reportUsage.setString(5, "ReportQuotaUsage_" + pid + "_" + txId + "_" + System.currentTimeMillis());

            // If we give up the user's balance and reservation are unchanged, so the next
            // call for them reports this usage instead
            if (executeWithLockRetry(reportUsage)) {
                updateUserBalance(users[randomuser], reportUsage, randomuser);
                shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
            } else {
                shc.reportLatency(ABANDONED_CALL, startMs, "Latency of abandoned call", 10000);
            }
        }

        users[randomuser].endTran();
    }

    /**
     * Copy the spendable balance and reservation returned by AddCredit / ReportQuotaUsage
     * into our client-side view of the user, so the next transaction for them knows
     * whether to top up or how much to report as used. NULL means 'unchanged'.
     *
     * @param user
     * @param statement a statement that has just been executed
     * @param sessionId
     * @throws SQLException
     */
    private static void updateUserBalance(UserTransactionState user, PreparedStatement statement, long sessionId) throws SQLException {
        try (ResultSet rs = statement.getResultSet()) {
            if (rs != null && rs.next()) {
                final byte statusByte = rs.getByte("l_status_byte");

                final long spendable = rs.getLong("l_spendable_balance");
                if (!rs.wasNull()) {
                    user.reportEndTransaction(sessionId, statusByte, spendable);
                }

                final long reserved = rs.getLong("l_currently_reserved");
                if (!rs.wasNull()) {
                    user.currentlyReserved = reserved;
                }
            }
        }
    }

    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

        oneLineSummary.append(tpMs);
        oneLineSummary.append(':');

        oneLineSummary.append(tps);
        oneLineSummary.append(':');

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, QUEUE_QUEUE);
        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, ATTEMPTS);
        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, QUEUE_LAG);
        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, REPORT_QUOTA_USAGE);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_PUT);
        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_GET);

        msg(oneLineSummary.toString());

        msg(shc.toString());
    }

    /**
     * Get Linux process ID - used for pseudo unique ids
     *
     * @return Linux process ID
     */
    private static long getPid() {
        return ProcessHandle.current().pid();
    }

    /**
     * Return a loyalty card number
     *
     * @param r
     * @param maxId
     * @return a random loyalty card number between 0 and maxId
     */
    private static long getNewLoyaltyCardNumber(Random r, int maxId) {
        return r.nextInt(maxId);
    }

    /**
     * get EXTRA_MS env variable if set
     *
     * @return extraMs
     */
    public static int getExtraMsIfSet() {

        int extraMs = 0;

        String extraMsEnv = System.getenv(EXTRA_MS);

        if (extraMsEnv != null && extraMsEnv.length() > 0) {
            msg("EXTRA_MS is '" + extraMsEnv + "'");
            extraMs = Integer.parseInt(extraMsEnv);
        }

        return extraMs;
    }

}
