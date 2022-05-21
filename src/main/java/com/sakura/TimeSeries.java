package com.sakura;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeries {

    static final Label INSTRUMENT = Label.label("Instrument");
    static final Label TIME_SERIES = Label.label("TimeSeries");


    @Context
    public Transaction tx;

    @Context
    public Log log;


    @Procedure(value = "sakura.timeseries", mode = Mode.READ)
    @Description("show timeSeries")
    public Stream<SakuraTImeSeriesResult> timeseries(@Name("Instrument") String instrument, @Name("Feed") String feed, @Name("TimeSeries") String timeSeries) {
        Map<LocalDate,SakuraTImeSeriesResult> result = new TreeMap<>();
        Node instr = tx.findNode(INSTRUMENT, "code", instrument);

        List<Node> connectedFeeds = Lists.newArrayList();
        instr.getRelationships(RelationshipType.withName("FEED")).forEach(r -> connectedFeeds.add(r.getEndNode()));
        Optional<Node> foundFeedNode = connectedFeeds.stream().filter(it -> it.getProperty("code").equals(feed)).findFirst();
        List<Node> connectedSeries = Lists.newArrayList();

        if (foundFeedNode.isPresent()) {
            foundFeedNode.get().getRelationships().forEach(
                    r -> {
                        if (r.getEndNode().hasLabel(TIME_SERIES)) {
                            connectedSeries.add(r.getEndNode());
                        }
                    }
            );
        }
        Optional<Node> foundSeries = connectedSeries.stream()
                .filter(it -> it.getProperty("name").equals(timeSeries)).findFirst();


        if (foundSeries.isPresent()) {
            TraversalDescription td = tx.traversalDescription()
                    .depthFirst()
                    .relationships(RelationshipType.withName("NEXT"), Direction.OUTGOING)
                    .evaluator(Evaluators.excludeStartPosition());
            Traverser traverser = td.traverse(foundSeries.get());
            traverser.iterator().forEachRemaining(
                    it -> {

                        List<SakuraTImeSeriesResult> revResults = getResultsForRevision(it.endNode());
                        revResults.forEach(res->result.put(res.date,res));

                    }
            );
        }
        return result.keySet().stream().map(it->result.get(it));
    }

    private List<SakuraTImeSeriesResult> getResultsForRevision(Node revisionNode) {
        List<SakuraTImeSeriesResult> results = Lists.newArrayList();
        log.info("getting ALL results for revision " + revisionNode.getProperty("version"));

        Node captureNode = revisionNode.getRelationships(RelationshipType.withName("CAPTURE")).iterator().next().getEndNode();

        TraversalDescription td = tx.traversalDescription()
                .depthFirst()
                .relationships(RelationshipType.withName("ON"), Direction.OUTGOING).evaluator(new Evaluator() {
                    @Override
                    public Evaluation evaluate(Path path) {
                        if (path.endNode().hasLabel(Label.label("Date"))) {
                            return Evaluation.INCLUDE_AND_PRUNE;
                        } else return Evaluation.EXCLUDE_AND_CONTINUE;
                    }
                });

        Traverser traverser = td.traverse(captureNode);
        traverser.iterator().forEachRemaining(it -> {
            String datePropertyStr = it.endNode().getProperty("date").toString().substring(0, 10);
            Long version = Long.parseLong(revisionNode.getProperty("version").toString());
            LocalDate ld = LocalDate.parse(datePropertyStr);
            SakuraTImeSeriesResult result = new SakuraTImeSeriesResult((Double) captureNode.getProperty("value"), version, ld);
            results.add(result);
        });
        return results;
    }


    public static class SakuraTImeSeriesResult {
        // These records contain two lists of distinct relationship types going in and out of a Node.

        public Double value;
        public Long version;
        public LocalDate date;


        public SakuraTImeSeriesResult(Double value, Long version, LocalDate date) {
            this.value = value;
            this.version = version;
            this.date = date;

        }


    }
}
