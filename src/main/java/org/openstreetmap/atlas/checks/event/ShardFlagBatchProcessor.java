package org.openstreetmap.atlas.checks.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.eventbus.Subscribe;
import com.google.gson.JsonObject;
import org.openstreetmap.atlas.checks.utility.CheckFlagSorter;
import org.openstreetmap.atlas.checks.utility.NamedCheckFlag;
import org.openstreetmap.atlas.event.EventService;
import org.openstreetmap.atlas.event.Processor;
import org.openstreetmap.atlas.event.ShutdownEvent;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.geojson.GeoJsonFeature;
import org.openstreetmap.atlas.geography.geojson.GeoJsonFeatureCollection;
import org.openstreetmap.atlas.geography.geojson.GeoJsonUtils;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.eventbus.AllowConcurrentEvents;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

public class ShardFlagBatchProcessor implements Processor<ShardFlagsBatchEvent>
{
    public static final String PRIORITY_REPORT_SUFFIX = "prioritizedAreas.geojson";
    private final ConcurrentHashMap<Shard, ConcurrentHashMap<String, AtomicLong>> stats;
    private final SparkFileHelper fileHelper;
    private final String directory;
    private final EventService eventService;
    private final CheckFlagSorter sorter;
    private final Integer numberOfPriority;

    public ShardFlagBatchProcessor(final SparkFileHelper fileHelper, final String directory, final
            EventService eventService, final Rectangle maxBounds, final Integer numberOfPriority)
    {
        this.fileHelper = fileHelper;
        this.directory = directory;
        this.eventService = eventService;
        this.sorter = new CheckFlagSorter( maxBounds, this.eventService);
        this.stats = new ConcurrentHashMap<>();
        this.eventService.register(this);
        this.numberOfPriority = numberOfPriority;
    }

    @Subscribe
    @AllowConcurrentEvents
    public void process(final CheckFlagEvent event)
    {
        this.sorter.add(new NamedCheckFlag(event.getCheckFlag(), event.getCheckName()));
    }

    @Override
    @AllowConcurrentEvents
    @Subscribe
    public void process(final ShardFlagsBatchEvent event)
    {
        this.recordBatchAndPrint(event.getShard(), event.getBatch());
    }

    private void recordBatchAndPrint(final Shard shard, final Collection<NamedCheckFlag> batch)
    {
        final StringBuffer buffer = new StringBuffer();
        this.stats.putIfAbsent(shard, new ConcurrentHashMap<>());
        batch.forEach(namedCheckFlag ->
        {
            this.stats.get(shard).putIfAbsent(namedCheckFlag.getName(),
                    new AtomicLong());
            this.stats.get(shard).get(namedCheckFlag.getName()).incrementAndGet();
            buffer.append(CheckFlagEvent
                    .flagToJson(namedCheckFlag.getFlag(),
                            Maps.stringMap("shard", shard.getName()))
                    .toString());
            buffer.append(System.lineSeparator());
        });
        this.fileHelper.write(this.directory, this.getFileName(shard), buffer.toString());
    }

    private String getFileName(final Shard shard)
    {
        return String.format("%s-%d.log", shard.getName(), new Date().getTime());
    }

    @Override
    @Subscribe
    public void process(final ShutdownEvent event)
    {
        this.sorter.getAllShardBucketCollectionPairs().forEach(this::recordBatchAndPrint);
        this.reportPriority();
    }

    public ToLongFunction<Map.Entry<String,AtomicLong>> scoreExtracter()
    {
        final HashSet<String> checksToCount = new HashSet<>(Arrays.asList("SinkIslandCheck","FloatingEdgeCheck","DuplicateLocationInPolyLineCheck","SelfIntersectingPolylineCheck","OrphanNodeCheck","OverlappingEdgeCheck","IntersectingBuildingsCheck","MalformedPolyLineCheck"));
        return entry ->
        {
            if(checksToCount.contains(entry.getKey()))
            {
                return entry.getValue().get();
            }else
            {
                return 0L;
            }
        };
    }

    /**
     *
     *
     * 1 DuplicateWaysCheck
     *    1 InvalidMultiPolygonRelationCheck
     *    1 SpikyBuildingCheck
     *    2 InvalidGeometryCheck
     *    2 LineCrossingBuildingCheck
     *    3 ConnectivityCheck
     *    3 SharpAngleCheck
     *    4 EdgeCrossingEdgeCheck
     *    5 BuildingRoadIntersectionCheck
     *    5 ValenceOneImportantRoadCheck
     *    6 MalformedPolyLineCheck
     *   10 IntersectingBuildingsCheck
     *   10 OverlappingEdgeCheck
     *   14 OrphanNodeCheck
     *   14 SelfIntersectingPolylineCheck
     *   33 DuplicateLocationInPolyLineCheck
     *   35 FloatingEdgeCheck
     *  133 SinkIslandCheck
     */
    public void reportPriority()
    {
        final PriorityQueue<Tuple<Shard, Long>> shardScores = new PriorityQueue<>((tuple1, tuple2) -> tuple2.getSecond().compareTo(tuple1.getSecond()));
        this.stats.entrySet().forEach(mapEntry ->
        {
            final Shard shard = mapEntry.getKey();
            final ConcurrentHashMap<String, AtomicLong> checkStats = mapEntry.getValue();
            // TODO, make this more flexible
            final Long score = checkStats.reduceEntriesToLong(1, scoreExtracter(), 0L, (long1, long2) -> long1 + long2);
            shardScores.add(Tuple.createTuple(shard,score));

        });

        final List<GeoJsonFeature> priorityAreas = new ArrayList<>(this.numberOfPriority);
        for ( int i = 1; i<= this.numberOfPriority; i++)
        {
            final Tuple<Shard, Long> shardScore = shardScores.poll();
            if (Objects.isNull(shardScore))
            {
                break;
            }
            priorityAreas.add(new GeoJsonFeature()
            {
                @Override
                public JsonObject asGeoJsonGeometry()
                {
                    return shardScore.getFirst().bounds().asGeoJsonGeometry();
                }

                @Override
                public JsonObject getGeoJsonProperties()
                {
                    final JsonObject properties = new JsonObject();
                    properties.addProperty("shard", shardScore.getFirst().getName());
                    properties.addProperty("score", shardScore.getSecond());
                    return properties;
                }
            });
        }
        this.fileHelper.write(this.directory, String.format("%d-", new Date().getTime()) + PRIORITY_REPORT_SUFFIX, GeoJsonUtils.featureCollection(
                new GeoJsonFeatureCollection<GeoJsonFeature>()
                {
                    @Override
                    public Iterable<GeoJsonFeature> getGeoJsonObjects()
                    {
                        return priorityAreas;
                    }

                    @Override
                    public JsonObject getGeoJsonProperties()
                    {
                        final JsonObject properties = new JsonObject();
                        //TODO update with flexible scoring
                        properties.addProperty("score_function","flag_count_sum");
                        if(priorityAreas.size() == 0)
                        {
                            properties.addProperty("warning","no flags produced or number of priority areas set to 0");
                        }
                        return properties;
                    }
                }).toString());
    }
}
