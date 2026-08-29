package com.thelastpickle.htap.producer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** The one HTTP call this process makes, kept apart so the poller can be tested without a port. */
final class Http {

    private Http() {}

    static SettingsPoller.Fetch get(Duration timeout) {
        HttpClient client = HttpClient.newBuilder().connectTimeout(timeout).build();
        return url -> {
            HttpRequest request =
                    HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
            HttpResponse<String> response;
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while asking " + url);
            }
            if (response.statusCode() / 100 != 2) {
                throw new IOException(url + " answered HTTP " + response.statusCode());
            }
            return response.body();
        };
    }
}
