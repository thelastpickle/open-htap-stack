package com.thelastpickle.htap.producer;

/**
 * Where this process says what it is doing.
 *
 * <p>Standard output with the Python's own {@code [producer]} prefix, because the compose logs and
 * the workflow read these lines; a logging framework would put a level and a thread in front of
 * them and change what a reader greps for. An interface so a test can read what was said.
 */
interface Log {

    Log STDOUT = line -> System.out.println("[producer] " + line);

    void say(String line);
}
