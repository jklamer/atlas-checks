package org.openstreetmap.atlas.checks.event;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;
import org.openstreetmap.atlas.checks.utility.CheckFlagSorter;
import org.openstreetmap.atlas.checks.utility.NamedCheckFlag;
import org.openstreetmap.atlas.event.EventService;
import org.openstreetmap.atlas.event.Processor;
import org.openstreetmap.atlas.event.ShutdownEvent;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.eventbus.AllowConcurrentEvents;

public class ShardFlagBatchProcessor implements Processor<ShardFlagsBatchEvent>
{
    public static final String PRIORITY_REPORT_NAME = "report.txt";
    private final ConcurrentHashMap<Shard, ConcurrentHashMap<String, AtomicLong>> stats;
    private final SparkFileHelper fileHelper;
    private final String directory;
    private final EventService eventService;
    private final CheckFlagSorter sorter;

    public ShardFlagBatchProcessor(final SparkFileHelper fileHelper, final String directory, final
            EventService eventService, final Rectangle maxBounds)
    {
        this.fileHelper = fileHelper;
        this.directory = directory;
        this.eventService = eventService;
        this.sorter = new CheckFlagSorter( maxBounds, this.eventService);
        this.stats = new ConcurrentHashMap<>();
        this.eventService.register(this);
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
                            Maps.stringMap("shard",shard.getName()))
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

    public void reportPriority()
    {

    }
}
