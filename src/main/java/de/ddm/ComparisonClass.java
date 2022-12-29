package de.ddm.actors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * A class representing the id of a column (consisting of the file index and the column index)
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class ComparisonClass {
    int fileId;
    int columnId;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ComparisonClass cID = (ComparisonClass) o;
        return fileId == cID.fileId && columnId == cID.columnId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, columnId);
    }

    public boolean isDifferentFile(ComparisonClass cID) {
        return cID.getFileId() != this.fileId;
    }

    @Override
    public String toString() {
        return "ColumnId{"+"fileId=" + fileId +", columnId=" + columnId +'}';
    }
}