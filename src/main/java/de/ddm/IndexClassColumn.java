package de.ddm;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class IndexClassColumn {
    // create hash Index for every column
    int file;         //file number
    int column;       // column number

    @Override
    // create hash id for column (file x column)
    public int hashCode() {return Objects.hash(file, column);}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        // empty object or diff. classes
        if (o == null || getClass() != o.getClass()) return false;
        IndexClassColumn index = (IndexClassColumn) o;
        return file == index.file && column == index.column;}

    @Override
    // string msg-debug
    public String toString() {return "ColumnIndex["+ file + " | " + column + ']';}
}