package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.Collectors;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.
        System.out.println("Total rows remaining after cleaning " + cleaned.size());

        // TODO 2:
        // Compute the average temperature across all valid rows.
        System.out.println("Average temp: " + aveTemp(cleaned));

        // TODO 3:
        // Find the city with the highest average temperature.
        System.out.println("City with the highest average temp: " + highestAveTemp(cleaned).get());

        // TODO 4:
        // Group records by city.
        groupByCity(cleaned);

        // TODO 5:
        // Compute average precipitation by city.
        getAveRain(cleaned);

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        cleaned.stream()
                .collect(Collectors.partitioningBy(x -> x.temperatureC() > 0));

        // TODO 7:
        // Create a Set<String> of all distinct cities.
        groupByCity(cleaned).keySet();

        // TODO 8:
        // Find the wettest single day.
        wettestDay(cleaned);

        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.

        cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::humidity)));

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        for (WeatherRecord weatherRecord : cleaned) {
            StringBuilder sb = new StringBuilder();
            sb.append(weatherRecord.city());
            sb.append(" on ");
            sb.append(weatherRecord.date());
            sb.append(": temperature: ");
            sb.append(weatherRecord.temperatureC());
            sb.append(", humidity: ");
            sb.append(weatherRecord.humidity());
            sb.append("%, precipitation: ");
            sb.append(weatherRecord.precipitationMm());
        }

        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }
    static double aveTemp(List<WeatherRecord> records) {
        return records.stream()
                .collect(Collectors.averagingDouble(WeatherRecord::temperatureC));
    }

    static Optional<String> highestAveTemp(List<WeatherRecord> records) {
        return records.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::temperatureC)))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    static Map<String, List<WeatherRecord>> groupByCity(List<WeatherRecord> records) {
        return records.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city));
    }

    static Map<String, Double> getAveRain(List<WeatherRecord> records) {
        return records.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::precipitationMm)));
    }

    static Optional<WeatherRecord> wettestDay(List<WeatherRecord> records) {
        return records.stream()
                .max(Comparator.comparingDouble(WeatherRecord::precipitationMm));
    }

    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails

        String[] columns = row.split(",");
        if (columns.length != 6) {
            return Optional.empty();
        }
        if (columns[3].trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            double temperature = Double.parseDouble(columns[3].trim());
            int humidity = Integer.parseInt(columns[4].trim());
            double precipitation = Double.parseDouble(columns[5].trim());
            return Optional.of(new WeatherRecord(columns[0].trim(), columns[1].trim(),
                    columns[2].trim(), temperature, humidity, precipitation));

        } catch (NumberFormatException e) {
            return Optional.empty();
        }


    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0


        if (r.temperatureC < -60 || r.temperatureC > 60) {
            return false;
        }
        if (r.humidity() < 0 || r.humidity() > 100) {
            return false;
        }
        if (r.precipitationMm() < 0) {
            return false;
        }
        return true;
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

}
