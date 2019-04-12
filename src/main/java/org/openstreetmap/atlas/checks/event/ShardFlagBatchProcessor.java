package org.openstreetmap.atlas.checks.event;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.openstreetmap.atlas.event.Processor;
import org.openstreetmap.atlas.event.ShutdownEvent;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.eventbus.AllowConcurrentEvents;

public class ShardFlagBatchProcessor implements Processor<ShardFlagsBatchEvent>
{

    private final ConcurrentHashMap<Shard, ConcurrentHashMap<String, AtomicLong>> stats;
    private final SparkFileHelper fileHelper;
    private final String directory;

    public ShardFlagBatchProcessor(final SparkFileHelper fileHelper, final String directory)
    {
        this.fileHelper = fileHelper;
        this.directory = directory;
        this.stats = new ConcurrentHashMap<>();
    }

    @Override
    @AllowConcurrentEvents
    public void process(final ShardFlagsBatchEvent event)
    {
        final StringBuffer buffer = new StringBuffer();
        this.stats.putIfAbsent(event.getShard(), new ConcurrentHashMap<>());
        event.getBatch().forEach(namedCheckFlag ->
        {
            this.stats.get(event.getShard()).putIfAbsent(namedCheckFlag.getName(),
                    new AtomicLong());
            this.stats.get(event.getShard()).get(namedCheckFlag.getName()).incrementAndGet();
            buffer.append(CheckFlagEvent
                    .flagToJson(namedCheckFlag.getFlag(),
                            Maps.stringMap("shard", event.getShard().getName()))
                    .toString());
            buffer.append(System.lineSeparator());
        });
        this.fileHelper.write(this.directory, this.getFileName(event.getShard()), buffer.toString());
    }

    private String getFileName(final Shard shard)
    {
        return String.format("%s-%d.log", shard.getName(), new Date().getTime());
    }

    @Override
    public void process(final ShutdownEvent event)
    {
    }
}
