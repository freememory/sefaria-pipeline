package org.freememory.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads a {@link PipelineConfig} from a JSON file.
 *
 * === Usage in a script ===
 *
 *   PipelineConfig config = ConfigLoader.load(args);
 *
 * This looks for a --config argument in the args array. If none is provided,
 * it falls back to config/pipeline.json in the current working directory.
 * If that also doesn't exist, default values from PipelineConfig are used.
 *
 * === Config file location ===
 *
 * Pass the path as a command-line argument:
 *
 *   java -jar sefaria-pipeline.jar --config config/download-p0.json
 *
 * See src/main/resources/config/ for example config files.
 *
 * === Partial configs ===
 *
 * Config files do not need to specify every field. Any field absent from
 * the JSON will retain its default value from PipelineConfig. This means
 * you can have a minimal config that only overrides what you need:
 *
 *   {
 *     "download": {
 *       "priorities": ["P0"],
 *       "concurrency": 10
 *     }
 *   }
 */
public class ConfigLoader
{
    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "config/pipeline.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Parse args for --config PATH, then load and return the config.
     * Falls back to defaults if no config file is found.
     */
    public static PipelineConfig load(String[] args)
    {
        Path configPath = findConfigPath(args);

        if (configPath == null || !Files.exists(configPath))
        {
            if (configPath != null)
            {
                log.warn("Config file not found: {}. Using defaults.", configPath);
            }
            else
            {
                log.info("No --config argument. Using defaults.");
            }
            return new PipelineConfig();
        }

        return loadFromFile(configPath);
    }

    /**
     * Load a PipelineConfig directly from a file path.
     */
    public static PipelineConfig loadFromFile(Path path)
    {
        try
        {
            log.info("Loading config from {}", path.toAbsolutePath());
            PipelineConfig config = mapper.readValue(path.toFile(), PipelineConfig.class);
            log.info("Config loaded successfully.");
            return config;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to load config from " + path + ": " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private static Path findConfigPath(String[] args)
    {
        if (args != null)
        {
            for (int i = 0; i < args.length - 1; i++)
            {
                if ("--config".equals(args[i]))
                {
                    return Path.of(args[i + 1]);
                }
            }
        }

        // Fall back to the conventional default location
        Path defaultPath = Path.of(DEFAULT_CONFIG_PATH);
        if (Files.exists(defaultPath))
        {
            log.info("Using default config at {}", defaultPath.toAbsolutePath());
            return defaultPath;
        }

        return null;
    }
}
