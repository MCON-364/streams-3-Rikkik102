package edu.touro.las.mcon364.ds;


import edu.touro.las.mcon364.streams.ds.WeatherDataScienceExercise;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static edu.touro.las.mcon364.streams.ds.WeatherDataScienceExercise.readCsvRows;
import static org.junit.jupiter.api.Assertions.*;

public class WeatherDSTest {

    //    record WeatherRecord(
//            String stationId,
//            String city,
//            String date,
//            double temperatureC,
//            int humidity,
//            double precipitationMm
//    ) {}
    @Test
    void testCorrectParseRow() {
        Optional<WeatherDataScienceExercise.WeatherRecord> wr = WeatherDataScienceExercise.parseRow("1222, new york, 10-01-2026, 34.7, 87, 0.6");
        assertTrue(wr.isPresent());
        assertEquals("1222", wr.get().stationId());
        assertEquals("new york", wr.get().city());
        assertEquals("10-01-2026", wr.get().date());
        assertEquals(34.7, wr.get().temperatureC());
        assertEquals(87, wr.get().humidity());
        assertEquals(0.6, wr.get().precipitationMm());
    }

    @Test
    void testTooFewColumns() {
        Optional<WeatherDataScienceExercise.WeatherRecord> wr = WeatherDataScienceExercise.parseRow("1222, 10-01-2026, 34.7, 87, 0.6");
        assertTrue(wr.isEmpty());
    }

    @Test
    void testMissingTemperature() {
        Optional<WeatherDataScienceExercise.WeatherRecord> wr = WeatherDataScienceExercise.parseRow("1222, new york, 10-01-2026, , 87, 0.6");
        assertTrue(wr.isEmpty());
    }

    @Test
    void testNonNumericTemperature() {
        Optional<WeatherDataScienceExercise.WeatherRecord> wr = WeatherDataScienceExercise.parseRow("1222, new york, 10-01-2026, asdf34.7, 87, 0.6");
        assertTrue(wr.isEmpty());
    }

    @Test
    void testValidTemperature() {
        WeatherDataScienceExercise.WeatherRecord wr = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 60, 23, 0.5);
        WeatherDataScienceExercise.WeatherRecord wre = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", -60, 23, 0.5);
        WeatherDataScienceExercise.WeatherRecord wrec = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 61, 23, 0.5);
        WeatherDataScienceExercise.WeatherRecord wreco = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", -61, 23, 0.5);
        assertTrue(WeatherDataScienceExercise.isValid(wr));
        assertTrue(WeatherDataScienceExercise.isValid(wre));
        assertFalse(WeatherDataScienceExercise.isValid(wrec));
        assertFalse(WeatherDataScienceExercise.isValid(wreco));
    }

    @Test
    void testValidHumidity() {
        WeatherDataScienceExercise.WeatherRecord wr = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, 0, 0.5);
        WeatherDataScienceExercise.WeatherRecord wre = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, 100, 0.5);
        WeatherDataScienceExercise.WeatherRecord wrec = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, -1, 0.5);
        WeatherDataScienceExercise.WeatherRecord wreco = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, 101, 0.5);
        assertTrue(WeatherDataScienceExercise.isValid(wr));
        assertTrue(WeatherDataScienceExercise.isValid(wre));
        assertFalse(WeatherDataScienceExercise.isValid(wrec));
        assertFalse(WeatherDataScienceExercise.isValid(wreco));
    }

    @Test
    void testValidPrecipitation() {
        WeatherDataScienceExercise.WeatherRecord wr = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, 23, 0.0);
        WeatherDataScienceExercise.WeatherRecord wre = new WeatherDataScienceExercise.WeatherRecord("122", "LA", "10-02-2026", 30, 23, -0.5);
        assertTrue(WeatherDataScienceExercise.isValid(wr));
        assertFalse(WeatherDataScienceExercise.isValid(wre));
    }

    //    After parsing and cleaning, the cleaned list is non-empty.
//    All records in the cleaned list pass isValid.
//    The city with the highest average temperature is a non-null, non-empty string.
//    The wettest single day has precipitation >= 0.
    @Test
    void testCleanedListIsPresent() throws IOException {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();
        assertFalse(cleaned.isEmpty());
    }

    @Test
    void testCleanedListIsValid() throws IOException {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();
        for (WeatherDataScienceExercise.WeatherRecord wr : cleaned) {
            assertTrue(WeatherDataScienceExercise.isValid(wr));
        }
    }

    @Test
    void testValidHighestAveTempCity() throws IOException {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();
        assertNotNull(WeatherDataScienceExercise.highestAveTemp(cleaned));
        assertNotEquals("", WeatherDataScienceExercise.highestAveTemp(cleaned));
    }

    @Test
    void testValidWettestDay() throws IOException {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();
        assertTrue(WeatherDataScienceExercise.wettestDay(cleaned).isPresent());
        assertTrue(WeatherDataScienceExercise.wettestDay(cleaned).get().precipitationMm() > 0.0);
    }
}