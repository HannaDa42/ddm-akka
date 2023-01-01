package de.ddm.homework;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Objects;


@AllArgsConstructor
@NoArgsConstructor
@Getter
public class InclusionHash {
    FileHash refTable;
    FileHash depTable;

    @Override
    // create hash id for column (file x column)
    public int hashCode() {return Objects.hash(refTable, depTable);}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InclusionHash index = (InclusionHash) o;
        return refTable.equals(index.refTable) && depTable.equals(index.depTable);
    }
}