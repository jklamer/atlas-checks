package org.openstreetmap.atlas.checks.utility;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

public class CheckFlagSorterTestRule extends CoreTestRule
{
    @TestAtlas(loadFromJosmOsmResource = "checkFlagSorterAtlas.osm")
    private Atlas atlas;

    public Atlas getAtlas()
    {
        return this.atlas;
    }
}
