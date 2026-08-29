package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * The digests {@code hashlib.blake2b} answers, which are what the embeddings in the table were
 * written with.
 *
 * <p>Every expected value here came from Python's own {@code hashlib}, so this is a comparison
 * against the function whose output is already stored rather than against a restatement of the
 * specification. The block boundary is covered on both sides and exactly on it: a message that is
 * a whole number of blocks has its last block flagged final rather than full, and getting that
 * wrong is the one error that agrees with the reference on almost every input.
 */
class Blake2bTest {

    /** RFC 7693's own test vector, at the full 64 bytes. */
    @Test
    void theRfcVectorForAbc() {
        assertEquals("ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d1"
                        + "7d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923",
                hex("abc", 64));
    }

    /** The empty message still takes one compression, of a block of nothing. */
    @Test
    void theEmptyMessageHasTheIvsOwnDigest() {
        assertEquals("e4a6a0577479b2b4", hex("", 8));
        assertEquals("786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419"
                        + "d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce",
                hex("", 64));
    }

    /** Eight bytes is what the embedder and the snippet digest both ask for. */
    @Test
    void eightByteDigestsAgreeWithHashlib() {
        assertEquals("40f89e395b66422f", hex("a", 8));
        assertEquals("d8bb14d833d59559", hex("abc", 8));
        assertEquals("4ea9080e08d569a3", hex("hydropower", 8));
        assertEquals("dabf4d9170171099", hex("renewable energy generation", 8));
        assertEquals("2bb50820defdfbf2", hex("the quick brown fox jumps over the lazy dog", 8));
    }

    /** A snippet of Wikipedia prose is several blocks long, so the counter has to advance. */
    @Test
    void aMessageEitherSideOfTheBlockBoundaryAgrees() {
        assertEquals("8e70a0af447c4ffc", hex("x".repeat(127), 8));
        assertEquals("901ce374e8e12402", hex("x".repeat(128), 8));
        assertEquals("18de14b9d8eea5a3", hex("x".repeat(129), 8));
        assertEquals("1350ad21b0246597", hex("x".repeat(256), 8));
    }

    /** Exactly two blocks, at the full width, where a mis-flagged final block shows up. */
    @Test
    void aWholeNumberOfBlocksIsFlaggedFinalRatherThanFull() {
        assertEquals("082b91ea2e15d1556d2ceefdd5af5d64d31b4e01aff1959724578876293825b2"
                        + "36ee8079173a0a38160d7d6685d6bca0bfb62c177b3599b8727d9173e2115b91",
                hex("x".repeat(128), 64));
    }

    @Test
    void aDigestLengthOutsideWhatTheParameterBlockHoldsIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> Blake2b.digest(new byte[0], 0));
        assertThrows(IllegalArgumentException.class, () -> Blake2b.digest(new byte[0], 65));
    }

    private static String hex(String message, int digestBytes) {
        return HexFormat.of().formatHex(
                Blake2b.digest(message.getBytes(StandardCharsets.UTF_8), digestBytes));
    }
}
