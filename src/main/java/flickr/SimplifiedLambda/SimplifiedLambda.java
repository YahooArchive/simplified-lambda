// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache 2.0 License
// See LICENSE.txt file in the project root folder for License terms.

package flickr.SimplifiedLambda;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by bjoshi on 8/17/15.
 */
public class SimplifiedLambda {
    public final static byte[] FAMILY = Bytes.toBytes("f");
    public final static byte[] REALTIME_COLUMN = Bytes.toBytes("REALTIME");
    public final static byte[] BULK_COLUMN = Bytes.toBytes("BULK");

    HTableInterface lambdaTable;
    public SimplifiedLambda(HTableInterface _lambdaTable)
    {
        this.lambdaTable = _lambdaTable;
    }

    static private byte[] stringToBytes(String input) { return Bytes.toBytes(input); }
    static private byte[] getEntityKey(String entity) { return stringToBytes(entity); }

    public void pushBulkEntry(String key, String value) throws IOException {
        byte[] entityKey = this.getEntityKey(key);

        Put insertOp = new Put(entityKey);
        insertOp.add(FAMILY, BULK_COLUMN, stringToBytes(value));
        this.lambdaTable.put(insertOp);
    }

    public void pushRealtimeEntry(String key, String value) throws IOException {
        byte[] entityKey = this.getEntityKey(key);

        Put insertOp = new Put(entityKey);
        insertOp.add(FAMILY, REALTIME_COLUMN, stringToBytes(value));
        this.lambdaTable.put(insertOp);
    }

    private Cell getCell(String key, byte[] column) throws IOException {
        byte[] rowKey = this.getEntityKey(key);

        Get entityRowGet = new Get(rowKey);
        Result result = this.lambdaTable.get(entityRowGet);

        Cell cell = result.getColumnLatestCell(FAMILY, column);
        return cell;
    }

    private String getValueFromCell(Cell cell) {
        return Bytes.toString(CellUtil.cloneValue(cell));
    }

    public String getItemForKey(String key, byte[] column) throws IOException {
        Cell cell = getCell(key, column);

        if (cell != null) {
            return getValueFromCell(cell);
        } else {
            throw new IOException("Cannot retrieve string from hbase");
        }
    }

    public long getTimestampForKey(String key, byte[] column) throws IOException {
        Cell cell = getCell(key, column);

        if (cell != null) {
            return cell.getTimestamp();
        } else {
            throw new IOException("Cannot retrieve string from hbase");
        }
    }

    public void cleaner() throws IOException {
        // algorithm:
        // iterate over all rows
        // foreach row:
        //   if hasRealtime && !hasBulk:
        //     move realtime to bulk
        //   else if hasRealtime && hasBulk:
        //     if bulkTimestamp>realtimeTimestamp:
        //       delete realtimeTimestamp
        //     else:
        //       move realtime to bulk
        //

        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        ResultScanner resultScanner = this.lambdaTable.getScanner(scan);

        for (Result result : resultScanner) {
            if (result.isEmpty())
                continue;
            String key = Bytes.toString(result.getRow());
            boolean hasBulk = false;
            boolean hasRealtime = false;

            Cell bulkCell = getCell(key, BULK_COLUMN);
            if (bulkCell != null)
                hasBulk = true;

            Cell realtimeCell = getCell(key, REALTIME_COLUMN);
            if (realtimeCell != null)
                hasRealtime = true;

            byte[] rowKey = this.getEntityKey(key);

            if (hasRealtime && !hasBulk) {
                // move realtime to bulk
                byte[] realtimeValue = CellUtil.cloneValue(realtimeCell);
                Put insertOp = new Put(rowKey);
                insertOp.add(FAMILY, BULK_COLUMN, realtimeValue);
                Delete deleteOp = new Delete(rowKey);
                deleteOp.deleteColumn(FAMILY, REALTIME_COLUMN);

                this.lambdaTable.put(insertOp);
                this.lambdaTable.delete(deleteOp);
            } else if (hasRealtime && hasBulk) {
                long bulkTimestamp = bulkCell.getTimestamp();
                long realtimeTimestamp = realtimeCell.getTimestamp();
                if (bulkTimestamp > realtimeTimestamp ) {
                    //delete realtimeTimestamp
                    Delete deleteOp = new Delete(rowKey);
                    deleteOp.deleteColumn(FAMILY, REALTIME_COLUMN);
                    this.lambdaTable.delete(deleteOp);
                } else {
                    // move realtime to bulk
                    byte[] realtimeValue = CellUtil.cloneValue(realtimeCell);
                    Put insertOp = new Put(rowKey);
                    insertOp.add(FAMILY, BULK_COLUMN, realtimeValue);
                    Delete deleteOp = new Delete(rowKey);
                    deleteOp.deleteColumn(FAMILY, REALTIME_COLUMN);
                    this.lambdaTable.put(insertOp);
                    this.lambdaTable.delete(deleteOp);
                }
            }
        }
    }

    public String combiner(String key) throws IOException {
        boolean hasBulk = false;
        boolean hasRealtime = false;

        Cell bulkCell = getCell(key, BULK_COLUMN);
        if (bulkCell != null)
            hasBulk = true;

        Cell realtimeCell = getCell(key, REALTIME_COLUMN);
        if (realtimeCell != null)
            hasRealtime = true;

        if (!hasBulk && !hasRealtime) {
            throw new IOException("Cannot retrieve string from hbase");
        } else if (hasBulk && !hasRealtime) {
            return getValueFromCell(bulkCell);
        } else if (!hasBulk && hasRealtime) {
            return getValueFromCell(realtimeCell);
        } else {
            long bulkTimestamp = bulkCell.getTimestamp();
            long realtimeTimestamp = realtimeCell.getTimestamp();
            if (realtimeTimestamp > bulkTimestamp) {
                return getValueFromCell(realtimeCell);
            } else {
                throw new IOException("Bulk timestamp newer than realtime: shouldn't happen!");
            }
        }
    }

    public String dumpTable() throws IOException {
        String res = "key\tbulk\trealtime\tcombined\n";
        res += "........................................\n";

        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        ResultScanner resultScanner = this.lambdaTable.getScanner(scan);

        for (Result result : resultScanner) {
            if (result.isEmpty())
                continue;
            String key = Bytes.toString(result.getRow());
            boolean hasBulk = false;
            boolean hasRealtime = false;

            Cell bulkCell = getCell(key, BULK_COLUMN);
            if (bulkCell != null)
                hasBulk = true;

            Cell realtimeCell = getCell(key, REALTIME_COLUMN);
            if (realtimeCell != null)
                hasRealtime = true;

            res += key;
            res += '\t';
            if (hasBulk) {
                res += getValueFromCell(bulkCell);
            } else {
                res += "None";
            }
            res += '\t';
            if (hasRealtime) {
                res += getValueFromCell(realtimeCell);
            } else {
                res += "None";
            }
            res += "\t\t";

            String combinedResult = combiner(key);
            res += combinedResult;
            res += '\n';
        }
        return res;
    }
}
