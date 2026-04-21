package org.freememory.pipeline.agent.tools;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.Duration;

/**
 * Hebrew calendar tools — wraps the Hebcal calendar and date-converter APIs.
 *
 * === APIs used ===
 *
 * Parasha + holidays:
 *   GET https://www.hebcal.com/hebcal?v=1&cfg=json&maj=on&min=on&nx=on&s=on
 *       &year=<year>&month=<month>
 *
 * Hebrew date converter:
 *   GET https://www.hebcal.com/converter?cfg=json&gy=<year>&gm=<month>&gd=<day>&g2h=1
 *
 * Upcoming holidays:
 *   GET https://www.hebcal.com/hebcal?v=1&cfg=json&maj=on&year=<year>
 *
 * All endpoints are free and require no API key.
 */
public class HebrewCalendarTools
{
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .build();

    // ------------------------------------------------------------------
    // Tools
    // ------------------------------------------------------------------

    @Tool("Get the weekly Torah portion (parasha) and Jewish calendar events "
        + "for a specific Gregorian month. Returns the parasha name, Rosh Chodesh, "
        + "holidays, and special Shabbatot.")
    public String getCalendar(
            @P("Four-digit Gregorian year, e.g. 2024.")
            int year,

            @P("Gregorian month number 1–12.")
            int month)
    {
        String url = "https://www.hebcal.com/hebcal?v=1&cfg=json"
                + "&maj=on&min=on&nx=on&s=on"
                + "&year=" + year
                + "&month=" + month;

        return fetchJson(url, "calendar for " + year + "-" + month);
    }

    @Tool("Get the weekly Torah portion (parasha) for today or a specific date.")
    public String getWeeklyParasha(
            @P("Date in YYYY-MM-DD format, or the word 'today'.")
            String date)
    {
        LocalDate d = "today".equalsIgnoreCase(date.trim())
                ? LocalDate.now()
                : LocalDate.parse(date.trim());

        String url = "https://www.hebcal.com/hebcal?v=1&cfg=json&s=on"
                + "&year=" + d.getYear()
                + "&month=" + d.getMonthValue();

        return fetchJson(url, "parasha for " + d);
    }

    @Tool("Convert a Gregorian (civil) date to its Hebrew calendar equivalent. "
        + "Returns the Hebrew year, month name, and day.")
    public String toHebrewDate(
            @P("Date in YYYY-MM-DD format.")
            String gregorianDate)
    {
        LocalDate d = LocalDate.parse(gregorianDate.trim());
        String url = "https://www.hebcal.com/converter?cfg=json"
                + "&gy=" + d.getYear()
                + "&gm=" + d.getMonthValue()
                + "&gd=" + d.getDayOfMonth()
                + "&g2h=1";

        return fetchJson(url, "Hebrew date for " + gregorianDate);
    }

    @Tool("Convert a Hebrew calendar date to its Gregorian equivalent.")
    public String toGregorianDate(
            @P("Hebrew year (e.g. 5784).")
            int hebrewYear,

            @P("Hebrew month name in English (e.g. 'Tishrei', 'Nisan', 'Adar I').")
            String hebrewMonth,

            @P("Day of the Hebrew month (1–30).")
            int hebrewDay)
    {
        String url = "https://www.hebcal.com/converter?cfg=json"
                + "&hy=" + hebrewYear
                + "&hm=" + URLEncoder.encode(hebrewMonth, StandardCharsets.UTF_8)
                + "&hd=" + hebrewDay
                + "&h2g=1";

        return fetchJson(url, "Gregorian date for " + hebrewDay + " " + hebrewMonth + " " + hebrewYear);
    }

    @Tool("Get major Jewish holidays for a given Gregorian year.")
    public String getHolidays(
            @P("Four-digit Gregorian year.")
            int year)
    {
        String url = "https://www.hebcal.com/hebcal?v=1&cfg=json&maj=on&year=" + year;
        return fetchJson(url, "holidays for " + year);
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
