package com.sakura;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.driver.internal.InternalResult;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TimeSeriesTest {
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() throws IOException {

        var sw = new StringWriter();
        try (var in = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/timeseries.cypher")))) {
            in.transferTo(sw);
            sw.flush();
        }

        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(com.sakura.TimeSeries.class)
                .withFixture(sw.toString())
                .build();
    }

    @Test
    public void testTimeSeries() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
            Session session = driver.session()) {

            // When
            InternalResult result =(InternalResult) session.run( "CALL sakura.timeseries('RUSS', 'Bloomberg','EOD') ");
            List<Record> records= result.list();
            assertThat(records.size()).isEqualTo(2);
            assertThat(records.get(0).get("version").asLong()).isEqualTo(0L);
            assertThat(records.get(0).get("value").asDouble()).isCloseTo(23.444, Percentage.withPercentage(0.2));
            assertThat(records.get(0).get("date").asLocalDate().getDayOfMonth()).isEqualTo(3);
            assertThat(records.get(1).get("version").asLong()).isEqualTo(1L);
            assertThat(records.get(1).get("value").asDouble()).isCloseTo(4.494, Percentage.withPercentage(0.2));
            assertThat(records.get(1).get("date").asLocalDate().getDayOfMonth()).isEqualTo(4);



        }

    }


    @AfterAll
    void closeNeo4j() {
        this.embeddedDatabaseServer.close();
    }


}