package org.openstreetmap.atlas.checks.event;

import java.util.Collection;

import org.openstreetmap.atlas.checks.utility.NamedCheckFlag;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.event.Event;

public class ShardFlagsBatchEvent extends Event
{
    private final Shard shard;
    private final Collection<NamedCheckFlag> batch;

    public ShardFlagsBatchEvent(final Shard shard, final Collection<NamedCheckFlag> batch)
    {
        super();
        this.shard = shard;
        this.batch = batch;
    }

    public Collection<NamedCheckFlag> getBatch()
    {
        return this.batch;
    }

    public Shard getShard()
    {
        return this.shard;
    }
}
