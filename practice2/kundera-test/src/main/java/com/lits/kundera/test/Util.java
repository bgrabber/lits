package com.lits.kundera.test;

import org.apache.hadoop.hbase.client.Scan;

import java.util.Arrays;

/**
 * Util class for scanning Hbase
 */
public class Util {
    private static final int MAX_KEY_BYTE = 0xff;

    public static Scan createKeyPrefixScan(final byte[] keyPrefix) {
        final Scan result;
        if ((keyPrefix == null) || (keyPrefix.length == 0)) {
            result = new Scan();
        } else {
            result = new Scan(keyPrefix, next(keyPrefix));
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
}
