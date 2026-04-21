package org.freememory.pipeline.agent.tools;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Common.Condition;
import io.qdrant.client.grpc.Common.FieldCondition;
import io.qdrant.client.grpc.Common.Filter;
import io.qdrant.client.grpc.Common.Match;
import io.qdrant.client.grpc.Points.ScrollPoints;
import io.qdrant.client.grpc.Points.ScrollResponse;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.JsonWithInt.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Direct Sefaria text lookup by canonical ref string.
 *
 * Unlike semantic search (which finds thematically related passages), this tool
 * retrieves the exact Sefaria passage identified by a known reference such as
 * "Shabbat 2a" or "Mishneh Torah Laws of Prayer 2:4".
 *
 * This is particularly useful when:
 *   - The user asks "What does Rashi say on Genesis 1:1?"  (known citation)
 *   - The agent retrieved a chunk whose {@code linked_refs} field points to a
 *     related passage that should also be fetched for context
 *   - A previous answer cited a source and the user asks for more detail on it
 *
 * === Implementation ===
 *
 * Uses the Qdrant Scroll API with a keyword filter on the {@code ref} payload
 * field.  The first matching point (highest score is arbitrary for exact match)
 * is returned as formatted text.
 *
 * Note: refs are stored exactly as chunked, so very long passages span multiple
 * chunks.  A ref like "Genesis 1" will match multiple verse-group chunks.
 * This tool returns only the first match; extend it to return all matches if
 * needed.
 */
public class SefariaLookupTools
{
    private static final Logger log = LoggerFactory.getLogger(SefariaLookupTools.class);

    private final QdrantClient qdrant;
    private final String       collectionName;

    public SefariaLookupTools(QdrantClient qdrant, String collectionName)
    {
        this.qdrant         = qdrant;
        this.collectionName = collectionName;
    }

    // ------------------------------------------------------------------
    // Tools
    // ------------------------------------------------------------------

    @Tool("Retrieve the exact text of a Sefaria passage by its canonical reference. "
        + "Use this when you know the specific citation (e.g. 'Shabbat 2a', "
        + "'Genesis 1:1', 'Mishneh Torah Laws of Prayer 2:4', "
        + "'Rashi on Genesis 1:1'). Returns the text and metadata.")
    public String lookupByRef(
            @P("The canonical Sefaria reference string. Use the standard format: "
             + "'BookName Chapter:Verse' or 'Tractate DafSide' for Talmud. "
             + "For commentaries: 'Rashi on BookName Chapter:Verse'.")
            String ref)
    {
        Filter filter = Filter.newBuilder()
                .addMust(Condition.newBuilder()
                        .setField(FieldCondition.newBuilder()
                                .setKey("ref")
                                .setMatch(Match.newBuilder().setKeyword(ref).build())
                                .build())
                        .build())
                .build();

        ScrollPoints scrollRequest = ScrollPoints.newBuilder()
                .setCollectionName(collectionName)
                .setFilter(filter)
                .setLimit(3)   // return up to 3 matching chunks (covers both en+he)
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
                .build();

        try
        {
            ScrollResponse response = qdrant.scrollAsync(scrollRequest).get();

            if (response.getResultList().isEmpty())
            {
                return "No text found for ref: \"" + ref + "\". "
                        + "Check that the reference format matches Sefaria conventions.";
            }

            StringBuilder sb = new StringBuilder();
            for (var point : response.getResultList())
            {
                Map<String, Value> payload = point.getPayload();
                String text     = str(payload, "text");
                String foundRef = str(payload, "ref");
                String title    = str(payload, "title");
                String lang     = str(payload, "language");

                if (text != null && !text.isBlank())
                {
                    sb.append(String.format("[%s | %s | %s]\n%s\n\n",
                            foundRef, title, lang, text));
                }
            }

            return sb.isEmpty()
                    ? "Found entry for \"" + ref + "\" but it has no text."
                    : sb.toString().strip();
        }
        catch (Exception e)
        {
            log.error("Qdrant lookup failed for ref '{}': {}", ref, e.getMessage());
            return "Error looking up ref \"" + ref + "\": " + e.getMessage();
        }
    }

    // ------------------------------------------------------------------
    // Helper
    // ------------------------------------------------------------------

    private static String str(Map<String, Value> payload, String key)
    {
        Value v = payload.get(key);
        return (v != null && v.hasStringValue()) ? v.getStringValue() : null;
    }
}
