package com.lits.kundera.test;

import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * Util class for scanning Hbase
 */
public class Util {
    private static final int MAX_KEY_BYTE = 0xff;
    private static final int DIGITS_IN_BYTE = 2;
    private static final String[] HEX_STRING = prepareHexStringTable();
    private static final int SIZE_IN_BYTES = 16;

    private static String[] prepareHexStringTable() {
        final String[] table = new String[MAX_KEY_BYTE + 1];
        for (int i = 0; i < (MAX_KEY_BYTE + 1); ++i) {
            table[i] = String.format("%02x", i);
        }
        return table;
    }

    public static Scan createKeyPrefixScan(final byte[] keyPrefix) {
        final Scan result;
        if ((keyPrefix == null) || (keyPrefix.length == 0)) {
            result = new Scan();
        } else {
            result = new Scan(keyPrefix);
        }
        return result;
    }

    public static String toString(final byte[] key, final int length) {
        final StringBuilder sb = new StringBuilder(length * DIGITS_IN_BYTE);
        if (key != null) {
            for (int i = 0; i < length; ++i) {
                sb.append(HEX_STRING[key[i] & MAX_KEY_BYTE]);
            }
        }
        return sb.toString();
    }


    /**
     * Get array of bytes from UUID.
     * @param uuid UUID.
     * @return Array of UUID bytes.
     */
    public static byte[] toBytes(final UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.allocate(SIZE_IN_BYTES);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    /**
     * Get UUID from array of bytes.
     * @param bytes UUID as array of bytes.
     * @return UUID.
     */
    public static UUID fromBytes(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final long ms = buffer.getLong();
        final long ls = buffer.getLong();
        return new UUID(ms, ls);
    }
}
