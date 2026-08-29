package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What {@link CqliteLibrary#load} does with a path that is not the library.
 *
 * <p>Runs everywhere, unlike {@link CqliteNativeTest}: a load that fails needs no library,
 * and this is the branch a developer machine takes first. What it holds is the translation
 * of the linker's {@link IllegalArgumentException} into a {@link CqliteException}, which
 * every caller of this module is written to catch; a JDK that changed the exception a
 * failed lookup throws would otherwise let a raw one escape with nothing here to say so.
 */
class CqliteLibraryLoadTest {

    @Test
    void aPathThatIsNotThereIsRefusedAndNamed(@TempDir Path directory) {
        Path absent = directory.resolve("libcqlite_datafusion.so");
        CqliteException refusal = assertThrows(CqliteException.class, () -> CqliteLibrary.load(absent));
        assertEquals(CqliteStatus.ERROR, refusal.status());
        assertTrue(
                refusal.getMessage().contains(absent.toString()),
                "the message names the path, and said: " + refusal.getMessage());
    }

    @Test
    void aFileThatIsNotALibraryIsRefusedAndNamed(@TempDir Path directory) throws IOException {
        Path notALibrary = Files.writeString(directory.resolve("libnonsense.so"), "not a shared object\n");
        CqliteException refusal =
                assertThrows(CqliteException.class, () -> CqliteLibrary.load(notALibrary));
        assertEquals(CqliteStatus.ERROR, refusal.status());
        assertTrue(
                refusal.getMessage().contains(notALibrary.toString()),
                "the message names the path, and said: " + refusal.getMessage());
    }
}
