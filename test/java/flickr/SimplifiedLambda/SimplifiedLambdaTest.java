// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache 2.0 License
// See LICENSE.txt file in the project root folder for License terms.

package flickr.SimplifiedLambda;

import utility.MockHTable;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.logging.Logger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by bjoshi on 8/17/15.
 */
public class SimplifiedLambdaTest {
    private static Logger LOG = Logger.getLogger(SimplifiedLambda.class.getName());

    private MockHTable createLambdaTable() {
        MockHTable lambdaTable = new MockHTable("lambdaTable");
        lambdaTable.addColumnFamily(new String(SimplifiedLambda.FAMILY));
        return lambdaTable;
    }

    @Test
    public void testTableCreate() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);
    }

    @Test
    public void testPushAndGetData() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushBulkEntry("bulk","0");
        simplifiedLambda.pushRealtimeEntry("realtime", "1");

        assertEquals(simplifiedLambda.getItemForKey("bulk", SimplifiedLambda.BULK_COLUMN),"0");
        assertEquals(simplifiedLambda.getItemForKey("realtime", SimplifiedLambda.REALTIME_COLUMN),"1");

        try {
            assertNotEquals(simplifiedLambda.getItemForKey("realtime", SimplifiedLambda.BULK_COLUMN), "0");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }

        try {
            assertNotEquals(simplifiedLambda.getItemForKey("bulk", SimplifiedLambda.REALTIME_COLUMN), "1");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }
    }

    @Test
    public void testTimestamps() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushBulkEntry("bulk","0");
        simplifiedLambda.pushRealtimeEntry("realtime", "1");

        long bulkTimestamp = simplifiedLambda.getTimestampForKey("bulk", SimplifiedLambda.BULK_COLUMN);
        long realtimeTimestamp = simplifiedLambda.getTimestampForKey("realtime", SimplifiedLambda.REALTIME_COLUMN);

        assertTrue(realtimeTimestamp>bulkTimestamp);
    }

    @Test
    public void testBulkOnly() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushBulkEntry("bulk","0");

        assertEquals(simplifiedLambda.combiner("bulk"), "0");

        simplifiedLambda.cleaner();

        assertEquals(simplifiedLambda.combiner("bulk"), "0");
        assertEquals(simplifiedLambda.getItemForKey("bulk", SimplifiedLambda.BULK_COLUMN), "0");
        try {
            assertNotEquals(simplifiedLambda.getItemForKey("bulk", SimplifiedLambda.REALTIME_COLUMN), "0");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }
    }

    @Test
    public void testRealtimeOnly() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushRealtimeEntry("realtime", "0");

        assertEquals(simplifiedLambda.combiner("realtime"), "0");

        simplifiedLambda.cleaner();

        assertEquals(simplifiedLambda.combiner("realtime"), "0");

        // check that realtime is pushed to bulk after cleaner
        assertEquals(simplifiedLambda.getItemForKey("realtime", SimplifiedLambda.BULK_COLUMN), "0");

        // assert that realtime column is pushed to bulk and is deleted
        try {
            assertNotEquals(simplifiedLambda.getItemForKey("realtime", SimplifiedLambda.REALTIME_COLUMN), "0");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }
    }

    @Test
    public void testBulkBeforeRealtime() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushBulkEntry("common","0");
        simplifiedLambda.pushRealtimeEntry("common", "1");

        assertEquals(simplifiedLambda.combiner("common"), "1");

        simplifiedLambda.cleaner();

        // check that realtime is pushed to bulk after cleaner
        assertEquals(simplifiedLambda.getItemForKey("common", SimplifiedLambda.BULK_COLUMN), "1");

        // assert that realtime column is pushed to bulk and is deleted
        try {
            assertNotEquals(simplifiedLambda.getItemForKey("common", SimplifiedLambda.REALTIME_COLUMN), "1");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }
    }

    @Test
    public void testRealtimeBeforeBulk() throws Exception {
        MockHTable lambdaTable = createLambdaTable();

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);
        assertTrue(simplifiedLambda instanceof SimplifiedLambda);

        simplifiedLambda.pushRealtimeEntry("common", "1");
        simplifiedLambda.pushBulkEntry("common","0");

        try {
            assertEquals(simplifiedLambda.combiner("common"), "1");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Bulk timestamp newer than realtime: shouldn't happen!");
        }
        simplifiedLambda.cleaner();

        // bulk timestamp is newer, so check that it's intact
        assertEquals(simplifiedLambda.getItemForKey("common", SimplifiedLambda.BULK_COLUMN), "0");

        // assert that realtime column is deleted
        try {
            assertNotEquals(simplifiedLambda.getItemForKey("common", SimplifiedLambda.REALTIME_COLUMN), "1");
        } catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: Cannot retrieve string from hbase");
        }
    }
}
