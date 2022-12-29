package de.ddm.actors.profiling;

import de.ddm.IndexClassColumn;
import de.ddm.structures.InclusionDependency;

public interface DepMapper {
    InclusionDependency isDep(IndexClassColumn thisId, IndexClassColumn depId);
}
