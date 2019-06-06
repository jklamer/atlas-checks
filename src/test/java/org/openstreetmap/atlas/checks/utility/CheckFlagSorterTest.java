package org.openstreetmap.atlas.checks.utility;

import com.google.common.eventbus.Subscribe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.checks.event.ShardFlagsBatchEvent;
import org.openstreetmap.atlas.checks.flag.CheckFlag;
import org.openstreetmap.atlas.event.EventService;
import org.openstreetmap.atlas.event.Processor;
import org.openstreetmap.atlas.event.ShutdownEvent;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.Point;
import org.openstreetmap.atlas.geography.atlas.items.Relation;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class CheckFlagSorterTest
{

    private static Rectangle testBounds = SlippyTile.forName("12-3622-1622").bounds();

    private static final class TestBatchProcessor implements Processor<ShardFlagsBatchEvent>
    {
        private int expectedBatches;
        private AtomicInteger batchesActual;
        private final Consumer<ShardFlagsBatchEvent> verifier;

        public TestBatchProcessor(final Consumer<ShardFlagsBatchEvent> verifier)
        {
            this.expectedBatches = 0;
            this.batchesActual = new AtomicInteger();
            this.verifier = verifier;
        }

        public TestBatchProcessor setExpectedBatches(final int expectedBatches)
        {
            this.expectedBatches = expectedBatches;
            return this;
        }

        @Override
        @Subscribe
        public void process(final ShutdownEvent event)
        {
            Assert.assertEquals("Did not get the right number of Batches", this.expectedBatches, this.batchesActual.get());
        }

        @Override
        @Subscribe
        public void process(final ShardFlagsBatchEvent event)
        {
            this.verifier.accept(event);
            this.batchesActual.incrementAndGet();
        }
    }

    @Rule
    public final CheckFlagSorterTestRule setup = new CheckFlagSorterTestRule();


    @Test
    public void testBatches()
    {
        final AtomicLong checkFlagId = new AtomicLong();
        final EventService eventService = EventService.get("testBatches");
        final Consumer<ShardFlagsBatchEvent> eventVerifier = shardFlagsBatchEvent -> Assert.assertEquals( shardFlagsBatchEvent.getShard(), SlippyTile.forName("13-7244-3244"));
        final TestBatchProcessor processor = new TestBatchProcessor(eventVerifier).setExpectedBatches(5);
        final CheckFlagSorter sorter = new CheckFlagSorter(testBounds, eventService);
        eventService.register(processor);


        final Edge edgeToFlag = (Edge) this.getEntityWithName(ItemType.EDGE, "edge1");

        for(int i = 0; i < CheckFlagSorter.BATCH_SIZE * 5 + 1; i++ )
        {
            sorter.add(new NamedCheckFlag(new CheckFlag(String.valueOf(checkFlagId.getAndIncrement()),
                    Collections.singleton(edgeToFlag), Collections.emptyList()), "MadeUpCheck"));
        }
        eventService.complete();
    }

    @Test
    public void testDifferentBucketBatches()
    {
        final AtomicLong checkFlagId = new AtomicLong();
        final EventService eventService = EventService.get("testDifferentBucketBatches");
        final Consumer<ShardFlagsBatchEvent> eventVerifier = shardFlagsBatchEvent -> Assert.assertEquals( shardFlagsBatchEvent.getShard(), SlippyTile.forName("13-7244-3244"));
        final TestBatchProcessor processor = new TestBatchProcessor(eventVerifier).setExpectedBatches(5);
        final CheckFlagSorter sorter = new CheckFlagSorter(testBounds, eventService);
        eventService.register(processor);

        final Edge edge1 = (Edge) this.getEntityWithName(ItemType.EDGE, "edge1");
        final Edge edge2 = (Edge) this.getEntityWithName(ItemType.EDGE, "edge2");
        final Edge edge3 = (Edge) this.getEntityWithName(ItemType.EDGE, "edge3");
        final Edge edge4 = (Edge) this.getEntityWithName(ItemType.EDGE, "edge4");

        final Point point1 = (Point) this.getEntityWithName(ItemType.POINT, "point1");
        final Point point2 = (Point) this.getEntityWithName(ItemType.POINT, "point2");
        final Point point3 = (Point) this.getEntityWithName(ItemType.POINT, "point3");

        final Relation edge1point1 = (Relation) this.getEntityWithName(ItemType.RELATION, "relation1");
        final Relation edges1Through3 = (Relation) this.getEntityWithName(ItemType.RELATION, "relation2");



    }

    public AtlasEntity getEntityWithName(final ItemType itemType, final String name)
    {
        return this.setup.getAtlas().entities(atlasEntity -> atlasEntity.getType().equals(itemType) && atlasEntity.getTag("name").filter(name::equals).isPresent()).iterator().next();
    }
}
