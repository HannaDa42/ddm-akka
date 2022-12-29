package de.ddm;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Objects;


@AllArgsConstructor
@NoArgsConstructor
@Getter
public class IndexUnaryIND {
    IndexClassColumn referencedIndex;
    IndexClassColumn dependentIndex;

    @Override
    // create hash id for column (file x column)
    public int hashCode() {return Objects.hash(referencedIndex, dependentIndex);}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUnaryIND index = (IndexUnaryIND) o;
        return referencedIndex.equals(index.referencedIndex) && dependentIndex.equals(index.dependentIndex);
    }
}