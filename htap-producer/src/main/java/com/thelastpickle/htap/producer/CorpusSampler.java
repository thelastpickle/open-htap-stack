package com.thelastpickle.htap.producer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * One to three adjacent sentences from a memory-mapped corpus.
 *
 * <p>Built for throughput rather than for correctness of sentence splitting: it seeks to a random
 * offset and scans a bounded window backwards to a sentence boundary and forwards to the end of
 * the third sentence. Splitting the whole corpus into sentences would cost a pass over 1 GB, and
 * the snippets only have to differ in meaning for the vector page to say anything.
 *
 * <p>Mapped rather than read, so that {@code enwik9} costs no heap: a 953 MB corpus is addressed
 * through the page cache and the process holds a window at a time.
 */
final class CorpusSampler implements TextSource, AutoCloseable {

    /** How far back a scan looks for the start of a sentence, and forward for the end of one. */
    private static final int SCAN_BACK = 2048;

    private static final int SCAN_FORWARD = 4096;

    /** Below this a corpus has nothing worth sampling, and every asset reports no text. */
    private static final int MIN_USEFUL_BYTES = 32;

    private final FileChannel channel;
    private final ByteBuffer corpus;
    private final int size;

    CorpusSampler(Path path) throws IOException {
        if (!Files.isReadable(path)) {
            // Named rather than left as a bare NoSuchFileException, because a TEXT_FILE of
            // /app/enwik9 is a supported configuration that needs one build argument.
            String hint = path.toString().endsWith("enwik9")
                    ? "; the image is built without it, so rebuild with --build-arg FETCH_ENWIK9=1"
                            + " or leave TEXT_FILE at /app/wikipedia.txt"
                    : "";
            throw new IOException("TEXT_FILE " + path + " is not in this image" + hint);
        }
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        long bytes = channel.size();
        // int, because the scan arithmetic is in int and a corpus above 2 GiB would wrap it.
        // enwik9 unpacks to 953 MB, so nothing supported reaches this.
        if (bytes > Integer.MAX_VALUE) {
            channel.close();
            throw new IOException("TEXT_FILE " + path + " is larger than 2 GiB: " + bytes);
        }
        this.size = (int) bytes;
        this.corpus = channel.map(FileChannel.MapMode.READ_ONLY, 0, bytes).asReadOnlyBuffer();
    }

    @Override
    public String sample(long seed) {
        if (size < MIN_USEFUL_BYTES) {
            return "";
        }
        // A generator per call, seeded by the caller: the snippet has to be a function of the
        // seed alone, so that a refresh gives new text and a repeat gives the same text.
        Random rng = new Random(seed);
        int sentences = 1 + rng.nextInt(3);
        int at = rng.nextInt(size);
        return extract(at, sentences);
    }

    /** The snippet at an offset, with its whitespace collapsed as the page shows it. */
    String extract(int at, int sentences) {
        int start = sentenceStart(at);
        return take(start, sentences).trim().replaceAll("\\s+", " ");
    }

    /**
     * Backwards to just after the nearest terminator, or to the start of the window.
     *
     * <p>A window start that is mid-sentence is the price of a bounded scan, and it shows as a
     * snippet beginning lower-case rather than as anything a reader would call wrong.
     */
    private int sentenceStart(int at) {
        int start = Math.max(0, at - SCAN_BACK);
        for (int i = at - 1; i >= start; i--) {
            if (isTerminator(corpus.get(i)) || corpus.get(i) == '\n') {
                return i + 1;
            }
        }
        return start;
    }

    /** Forwards until the requested number of terminators, or to the end of the window. */
    private String take(int from, int sentences) {
        int end = Math.min(size, from + SCAN_FORWARD);
        int found = 0;
        int cut = end;
        for (int i = from; i < end; i++) {
            if (isTerminator(corpus.get(i))) {
                found++;
                if (found >= sentences) {
                    cut = i + 1;
                    break;
                }
            }
        }
        byte[] bytes = new byte[cut - from];
        corpus.duplicate().position(from).get(bytes);
        // Permissively: a corpus like enwik9 carries markup and bytes that are no valid UTF-8,
        // and one malformed sequence must not cost the whole snippet.
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static boolean isTerminator(byte c) {
        return c == '.' || c == '?' || c == '!';
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
