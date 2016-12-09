package com.lits.kundera.test;

import org.apache.hadoop.hbase.client.Scan;

import java.util.Arrays;

/**
 * Util class for scanning Hbase
 */
public class Util {
    private static final int MAX_KEY_BYTE = 0xff;
    private static final int DIGITS_IN_BYTE = 2;
    private static final String[] HEX_STRING = prepareHexStringTable();

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

    private static byte[] next(final byte[] key) {

        byte[] nextKey = Arrays.copyOf(key, key.length);
        boolean overflow = true;
        for (int i = nextKey.length - 1; i >= 0; --i) {
            ++nextKey[i];
            if (nextKey[i] != 0) {
                overflow = false;
                break;
            }
        }

        if (overflow) {
            nextKey = Arrays.copyOf(key, key.length + 1);
            nextKey[key.length] = (byte) MAX_KEY_BYTE;
        }
        return nextKey;
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
}
