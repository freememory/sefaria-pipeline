package org.freememory.pipeline.agent.tools;

import dev.langchain4j.agent.tool.Tool;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * Cross-cutting date/time and location tools available to every leaf agent.
 *
 * === Why this is a global tool ===
 *
 * Many Hebrew calendar and zmanim tools require the current date as a parameter
 * (e.g. getWeeklyParasha, getZmanim).  The LLM cannot know today's date from
 * its training data alone — it must call this tool first to ground itself in
 * real time before making any date-dependent query.
 *
 * Similarly, zmanim calculations require a geographic location.  The user's
 * default location is stored in the agent config and exposed here so any agent
 * can pass the correct city to Hebcal without asking the user every time.
 *
 * === Typical usage pattern ===
 *
 *   User: "What parsha are we reading this Shabbat?"
 *
 *   LLM  → getCurrentDateTime()
 *   Java → "Today is Friday, 18 April 2026 (20 Nisan 5786). Time: 20:31 IDT (Asia/Jerusalem)."
 *   LLM  → getWeeklyParasha("2026-04-18")
 *   Java → Hebcal response with parsha name
 *   LLM  → "This Shabbat we read Parshat Acharei Mot."
 */
public class DateTimeTools
{
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final String defaultCity;
    private final String defaultTimezone;
    private final HttpClient http;

    /**
     * @param defaultCity      city name for zmanim lookups, e.g. "Jerusalem"
     * @param defaultTimezone  IANA timezone ID, e.g. "Asia/Jerusalem" or "America/New_York"
     */
    public DateTimeTools(String defaultCity, String defaultTimezone)
    {
        this.defaultCity     = defaultCity;
        this.defaultTimezone = defaultTimezone != null ? defaultTimezone : "UTC";
        this.http = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
    }

    // ------------------------------------------------------------------
    // Tools
    // ------------------------------------------------------------------

    @Tool("Get the current date and time, including the Hebrew calendar date. "
        + "Always call this first before any query that depends on today's date, "
        + "the current week's parasha, upcoming holidays, or zmanim (prayer times).")
    public String getCurrentDateTime()
    {
        ZoneId zone;
        try
        {
            zone = ZoneId.of(defaultTimezone);
        }
        catch (Exception e)
        {
            zone = ZoneId.of("UTC");
        }

        ZonedDateTime now = ZonedDateTime.now(zone);

        String dayName  = now.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
        String dateFmt  = now.format(DateTimeFormatter.ofPattern("d MMMM yyyy", Locale.ENGLISH));
        String timeFmt  = now.format(DateTimeFormatter.ofPattern("HH:mm", Locale.ENGLISH));
        String tzAbbr   = now.format(DateTimeFormatter.ofPattern("z", Locale.ENGLISH));
        String isoDate  = now.format(DateTimeFormatter.ISO_LOCAL_DATE);

        // Fetch the Hebrew date from Hebcal
        String hebrewDate = fetchHebrewDate(now);

        return String.format(
                "Today is %s, %s (%s). Current time: %s %s (timezone: %s). " +
                "ISO date for API calls: %s.",
                dayName, dateFmt, hebrewDate, timeFmt, tzAbbr, defaultTimezone, isoDate
        );
    }

    @Tool("Get the user's configured default location for zmanim and Shabbat time calculations. "
        + "Returns the city name and timezone. Use this city when calling getZmanim or "
        + "getShabbatTimes if the user has not specified a different location.")
    public String getDefaultLocation()
    {
        return String.format(
                "Default location: %s (timezone: %s). "
                + "Use this city for zmanim calculations unless the user specifies otherwise.",
                defaultCity, defaultTimezone
        );
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Fetch the Hebrew calendar date for a given moment via the Hebcal converter API.
     * Returns a fallback string on failure so getCurrentDateTime() never throws.
     */
    private String fetchHebrewDate(ZonedDateTime dt)
    {
        try
        {
            String url = "https://www.hebcal.com/converter?cfg=json"
                    + "&gy=" + dt.getYear()
                    + "&gm=" + dt.getMonthValue()
                    + "&gd=" + dt.getDayOfMonth()
                    + "&g2h=1";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(TIMEOUT)
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = http.send(
                    request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200)
            {
                // Extract the "hd", "hm", "hy" fields from the JSON.
                // Avoids a full Jackson dependency in this class.
                String body = response.body();
                String hd = extractJsonValue(body, "hd");
                String hm = extractJsonValue(body, "hm");
                String hy = extractJsonValue(body, "hy");
                if (hd != null && hm != null && hy != null)
                {
                    return hd + " " + hm + " " + hy;
                }
            }
        }
        catch (Exception ignored) { /* non-fatal */ }

        return "Hebrew date unavailable";
    }

    /** Minimal JSON string/number extractor — avoids a full ObjectMapper dependency. */
    private static String extractJsonValue(String json, String key)
    {
        String search = "\"" + key + "\":";
        int idx = json.indexOf(search);
        if (idx < 0)
        {
            return null;
        }
        int start = idx + search.length();
        // Skip whitespace
        while (start < json.length() && json.charAt(start) == ' ')
        {
            start++;
        }
        if (start >= json.length())
        {
            return null;
        }
        // String value
        if (json.charAt(start) == '"')
        {
            int end = json.indexOf('"', start + 1);
            return end > start ? json.substring(start + 1, end) : null;
        }
        // Numeric value
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-'))
        {
            end++;
        }
        return end > start ? json.substring(start, end) : null;
    }
}
