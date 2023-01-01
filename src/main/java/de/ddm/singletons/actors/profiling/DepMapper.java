package de.ddm.singletons.actors.profiling;

import de.ddm.homework.FileHash;
import de.ddm.structures.InclusionDependency;

public interface DepMapper {
    InclusionDependency isDep(FileHash thisId, FileHash depId);
}
