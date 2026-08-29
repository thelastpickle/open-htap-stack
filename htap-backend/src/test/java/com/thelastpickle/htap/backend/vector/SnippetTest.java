package com.thelastpickle.htap.backend.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SnippetTest {

    /**
     * The three digests come from Python's {@code hashlib.blake2b(digest_size=8).hexdigest()},
     * which is what the Python's own {@code _snippet_key} returned.
     */
    @Test
    void aDigestIsTheEightByteHexPythonProduced() {
        assertEquals("cef9e849a53598b3", new Snippet("d1", "the drone climbed").digest());
        assertEquals("40f89e395b66422f", new Snippet("d1", "a").digest());
        assertEquals("e4a6a0577479b2b4", new Snippet("d1", "").digest());
    }

    @Test
    void aDifferentSnippetGetsADifferentDigest() {
        assertFalse(new Snippet("d1", "one").digest().equals(new Snippet("d1", "two").digest()));
    }

    /** Whitespace is not prose, and the loop must not spend an embedding on it. */
    @Test
    void onlyANonBlankSnippetIsPresent() {
        assertTrue(new Snippet("d1", "text").present());
        assertFalse(new Snippet("d1", "").present());
        assertFalse(new Snippet("d1", "  \n ").present());
        assertFalse(new Snippet("d1", null).present());
    }
}
