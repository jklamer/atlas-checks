package org.openstreetmap.atlas.checks.utility;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.openstreetmap.atlas.checks.event.EventService;
import org.openstreetmap.atlas.checks.event.ShardFlagsBatchEvent;
import org.openstreetmap.atlas.checks.flag.FlaggedObject;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.ShardBucketCollection;

public class CheckFlagSorter extends ShardBucketCollection<NamedCheckFlag, HashSet<NamedCheckFlag>>
{
    private static final int INITIAL_CAPACITY = 1000;
    public static final int BATCH_SIZE = 1000;
    private static final int ZOOM_LEVEL = 12;

    private final EventService eventService;

    public CheckFlagSorter(final Rectangle maximumBounds, final EventService eventService)
    {
        super(maximumBounds, ZOOM_LEVEL);
        this.eventService = eventService;
    }

    @Override
    protected boolean allowMultipleBucketInsertion()
    {
        return false;
    }

    @Override
    protected boolean addFunction(final NamedCheckFlag item,
            final HashSet<NamedCheckFlag> collection, final Shard shard)
    {
        synchronized (collection)
        {
            if (collection.size() >= BATCH_SIZE)
            {
                this.eventService.post(new ShardFlagsBatchEvent(shard, collection));
            }
            collection.clear();
            return super.addFunction(item, collection, shard);
        }
    }

    @Override
    protected HashSet<NamedCheckFlag> initializeBucketCollection()
    {
        return new HashSet<>(INITIAL_CAPACITY);
    }

    @Override
    protected Shard resolveShard(final NamedCheckFlag namedCheckFlag,
            final List<? extends Shard> possibleBuckets)
    {
        final MultiPolygon objectsMultipolygon = MultiPolygon.forOuters(Iterables
                .translate(namedCheckFlag.getFlag().getFlaggedObjects(), FlaggedObject::bounds));
        final HashMap<Shard, Integer> candidates = new HashMap<>();
        for (final Shard shard : possibleBuckets)
        {
            candidates.put(shard, (int) objectsMultipolygon.outers().stream()
                    .filter(objectBounds -> objectBounds.overlaps(shard.bounds())).count());
        }

        return candidates.keySet().stream().filter(shard -> candidates.get(shard) > 0)
                .sorted((shard1, shard2) -> candidates.get(shard2) - candidates.get(shard1))
                .sorted(Comparator.comparing(Shard::getName)).findFirst()
                .orElseThrow(() -> new CoreException(
                        "Check Flag {} bounds intersects with shards that no individual objects' bounds intersect with.",
                        namedCheckFlag));
    }
}
