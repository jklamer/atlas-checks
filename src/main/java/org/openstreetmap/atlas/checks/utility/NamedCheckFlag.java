package org.openstreetmap.atlas.checks.utility;

import org.openstreetmap.atlas.checks.base.Check;
import org.openstreetmap.atlas.checks.flag.CheckFlag;
import org.openstreetmap.atlas.geography.Located;
import org.openstreetmap.atlas.geography.Rectangle;

import java.io.Serializable;

/**
 * A located wrapper used for sorting {@link org.openstreetmap.atlas.checks.flag.CheckFlag}s while keeping their name;
 */
public class NamedCheckFlag implements Located, Serializable
{
    private final String name;
    private final CheckFlag flag;

    public NamedCheckFlag(final CheckFlag flag, final String name)
    {
        this.flag = flag;
        this.name = name;
    }

    public String getName()
    {
        return this.name;
    }

    public CheckFlag getFlag()
    {
        return this.flag;
    }

    @Override
    public Rectangle bounds()
    {
        return this.getFlag().bounds();
    }
}
