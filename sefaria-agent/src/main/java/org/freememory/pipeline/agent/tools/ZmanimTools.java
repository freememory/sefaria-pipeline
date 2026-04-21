package org.freememory.pipeline.agent.tools;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.Duration;

/**
 * Halachic zmanim (times) tool — wraps the Hebcal zmanim REST API.
 *
 * === API used ===
 *
 *   GET https://www.hebcal.com/zmanim?cfg=json&city=<city>&date=<YYYY-MM-DD>
 *
 * Hebcal's free public API returns JSON with fields like:
 *   dawn, sunrise, sofZmanShma_GRA, sofZmanTfilla_GRA, chatzot, minchaGedola,
 *   minchaKetana, plagHaMincha, sunset, dusk, tzait72, etc.
 *
 * No API key is required for the public endpoint.
 *
 * === Adding support for MyZmanim instead ===
 *
 * MyZmanim (myzmanim.com) offers more granular location support (lat/long).
 * Replace the buildUrl() method with the MyZmanim endpoint and adjust the
 * JSON field names accordingly.
 */
public class ZmanimTools
{
    private static final Logger log = LoggerFactory.getLogger(ZmanimTools.class);

    private static final String BASE_URL = "https://www.hebcal.com/zmanim";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .build();

    // ------------------------------------------------------------------
    // Tools
    // ------------------------------------------------------------------

    @Tool("Get halachic times (zmanim) for a location and date. Returns sunrise, "
        + "sunset, candle lighting time, latest times for Shema and Tefilla, "
        + "Mincha, Plag HaMincha, Tzait Hakochavim, and more.")
    public String getZmanim(
            @P("City name recognised by Hebcal, e.g. 'Jerusalem', 'New York', "
             + "'London', 'Tel Aviv'. Use English city names.")
            String city,

            @P("Date in YYYY-MM-DD format, or the word 'today' for today's date.")
            String date)
    {
        log.info("getZmanim({}, {})", city, date);
        String resolvedDate = "today".equalsIgnoreCase(date.trim())
                ? LocalDate.now().toString()
                : date.trim();

        String url = BASE_URL
                + "?cfg=json"
                + "&city=" + URLEncoder.encode(city, StandardCharsets.UTF_8)
                + "&date=" + URLEncoder.encode(resolvedDate, StandardCharsets.UTF_8);

        return fetchJson(url, "zmanim for " + city + " on " + resolvedDate);
    }

    @Tool("Get candle lighting and Havdalah times for Shabbat in a given city. "
        + "Also returns the weekly Torah portion name.")
    public String getShabbatTimes(
            @P("City name recognised by Hebcal, e.g. 'Jerusalem', 'New York'.")
            String city)
    {
        String url = "https://www.hebcal.com/shabbat?cfg=json"
                + "&city=" + URLEncoder.encode(city, StandardCharsets.UTF_8);

        return fetchJson(url, "Shabbat times for " + city);
    }

    // ------------------------------------------------------------------
    // HTTP helper
    // ------------------------------------------------------------------

    private String fetchJson(String url, String description)
    {
        try
        {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(TIMEOUT)
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = http.send(
                    request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200)
            {
                return "Error fetching " + description
                        + ": HTTP " + response.statusCode();
            }
            return response.body();
        }
        catch (Exception e)
        {
            return "Error fetching " + description + ": " + e.getMessage();
        }
    }
}
