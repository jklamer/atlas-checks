package org.openstreetmap.atlas.checks.utility;

import com.google.common.eventbus.Subscribe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.checks.event.ShardFlagsBatchEvent;
import org.openstreetmap.atlas.event.Processor;
import org.openstreetmap.atlas.event.ShutdownEvent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class CheckFlagSorterTest
{

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

        public void setExpectedBatches(final int expectedBatches)
        {
            this.expectedBatches = expectedBatches;
        }

        @Override
        @Subscribe
        public void process(final ShutdownEvent event)
        {
            Assert.assertEquals(this.expectedBatches, this.batchesActual.get());
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
    public final static CheckFlagSorterTestRule setup = new CheckFlagSorterTestRule();


    @Test
    public void testSort()
    {
        
    }
}
