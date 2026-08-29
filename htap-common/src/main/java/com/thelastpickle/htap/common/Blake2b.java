package com.thelastpickle.htap.common;

/**
 * BLAKE2b (RFC 7693), unkeyed, at whatever digest length a caller asks for.
 *
 * <p>Here because the JDK has no BLAKE2 and the vector demo needs this exact function, not merely
 * a good hash. Two things depend on the bytes. The hashing embedder maps each token to one of
 * 1536 dimensions through the digest, so a vector written by one process is comparable with a
 * query embedded by another only if both hash a token to the same dimension; the embeddings a
 * running stack already holds were written by {@code hashlib.blake2b(digest_size=8)}, and a
 * different function would leave them in the table ranking as noise. The live embedder's
 * per-asset digest is the second, and it is the same call.
 *
 * <p>Truncating SHA-256 would hash just as well and was rejected for that reason alone: it would
 * silently invalidate every row already indexed.
 *
 * <p>One shot rather than a {@code MessageDigest}: every caller here has the whole input in hand,
 * and a streaming interface would carry state this needs nowhere.
 */
public final class Blake2b {

    /** The initialisation vector, which is SHA-512's. */
    private static final long[] IV = {
        0x6a09e667f3bcc908L, 0xbb67ae8584caa73bL, 0x3c6ef372fe94f82bL, 0xa54ff53a5f1d36f1L,
        0x510e527fade682d1L, 0x9b05688c2b3e6c1fL, 0x1f83d9abfb41bd6bL, 0x5be0cd19137e2179L,
    };

    /** The message word each mixing step reads, one row per round. */
    private static final byte[][] SIGMA = {
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
        {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
        {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
        {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
        {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
        {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
        {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
        {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
        {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
        {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
        {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    };

    private static final int BLOCK_BYTES = 128;
    private static final int MOST_DIGEST_BYTES = 64;

    private Blake2b() {}

    /**
     * The digest of {@code message}, {@code digestBytes} long.
     *
     * @param digestBytes 1 to 64, as the algorithm's parameter block allows
     */
    public static byte[] digest(byte[] message, int digestBytes) {
        if (digestBytes < 1 || digestBytes > MOST_DIGEST_BYTES) {
            throw new IllegalArgumentException(
                    "digest length must be 1 to 64 bytes, got " + digestBytes);
        }
        long[] h = IV.clone();
        // The parameter block, folded into the first state word: digest length, key length of
        // zero, one fanout, one depth.
        h[0] ^= 0x01010000L ^ digestBytes;

        long counted = 0;
        int at = 0;
        // Every block but the last, which is compressed below whether or not any bytes remain:
        // the empty message is one padded block, and a message that is a whole number of blocks
        // must have its final one flagged as final rather than as full.
        while (message.length - at > BLOCK_BYTES) {
            counted += BLOCK_BYTES;
            compress(h, message, at, counted, false);
            at += BLOCK_BYTES;
        }
        byte[] last = new byte[BLOCK_BYTES];
        System.arraycopy(message, at, last, 0, message.length - at);
        counted += message.length - at;
        compress(h, last, 0, counted, true);

        byte[] digest = new byte[digestBytes];
        for (int i = 0; i < digestBytes; i++) {
            digest[i] = (byte) (h[i / 8] >>> (8 * (i % 8)));
        }
        return digest;
    }

    private static void compress(long[] h, byte[] block, int at, long counted, boolean last) {
        long[] m = new long[16];
        for (int i = 0; i < 16; i++) {
            m[i] = word(block, at + 8 * i);
        }
        long[] v = new long[16];
        System.arraycopy(h, 0, v, 0, 8);
        System.arraycopy(IV, 0, v, 8, 8);
        v[12] ^= counted;
        // v[13] would take the counter's high half, which no input here comes near: 2^64 bytes.
        if (last) {
            v[14] = ~v[14];
        }
        for (byte[] s : SIGMA) {
            mix(v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
            mix(v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
            mix(v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
            mix(v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
            mix(v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
            mix(v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
            mix(v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
            mix(v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
        }
        for (int i = 0; i < 8; i++) {
            h[i] ^= v[i] ^ v[i + 8];
        }
    }

    private static void mix(long[] v, int a, int b, int c, int d, long x, long y) {
        v[a] = v[a] + v[b] + x;
        v[d] = Long.rotateRight(v[d] ^ v[a], 32);
        v[c] = v[c] + v[d];
        v[b] = Long.rotateRight(v[b] ^ v[c], 24);
        v[a] = v[a] + v[b] + y;
        v[d] = Long.rotateRight(v[d] ^ v[a], 16);
        v[c] = v[c] + v[d];
        v[b] = Long.rotateRight(v[b] ^ v[c], 63);
    }

    /** One little-endian 64-bit word of the block. */
    private static long word(byte[] block, int at) {
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (block[at + i] & 0xFFL);
        }
        return value;
    }
}
