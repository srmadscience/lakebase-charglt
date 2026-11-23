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


/**
 * Possible response codes.
 *
 */
public class ReferenceData {

    public static final byte STATUS_OK = 42;

    public static final byte STATUS_NO_MONEY = 43;
    public static final byte STATUS_SOME_UNITS_ALLOCATED = 44;
    public static final byte STATUS_ALL_UNITS_ALLOCATED = 45;
    public static final byte STATUS_TXN_ALREADY_HAPPENED = 46;
    public static final byte STATUS_USER_DOESNT_EXIST = 50;
    public static final byte STATUS_RECORD_ALREADY_SOFTLOCKED = 53;
    public static final byte STATUS_RECORD_HAS_BEEN_SOFTLOCKED = 54;
    public static final byte STATUS_CREDIT_ADDED = 56;

    public static final int LOCK_TIMEOUT_MS = 50;

    /**
     * PostgreSQL's lock_not_available: the user's row is locked by another transaction and
     * the function, which uses FOR UPDATE NOWAIT, didn't wait for it.
     */
    public static final String SQLSTATE_LOCK_NOT_AVAILABLE = "55P03";

    /**
     * How many times to retry a call that fails because the user's row is locked.
     */
    public static final int MAX_LOCK_RETRIES = 10;

    /**
     * Upper limit of the random delay before the first retry. Doubles for each further
     * retry, up to LOCK_TIMEOUT_MS.
     */
    public static final int LOCK_RETRY_MS = 5;

}
