package wiadrodanych.utils;

public class EnvTools {
    public static final String INPUT_TOPIC = "INPUT_TOPIC";
    public static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
    public static final String APPLICATION_ID_CONFIG = "APPLICATION_ID_CONFIG";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "BOOTSTRAP_SERVERS_CONFIG";

    public static String getEnvValue(String environmentKey, String defaultValue)
    {
        String envValue = System.getenv(environmentKey);
        if(envValue != null && !envValue.isEmpty())
        {
            return envValue;
        }
        return defaultValue;
    }
}
