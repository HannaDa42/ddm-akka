package de.ddm.homework;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FileHash implements Comparable{
    // create hash Index for every column
    int file;
    int entry;

    @Override
    // create hash id for (file x column)
    public int hashCode() {return Objects.hash(file, entry);}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        // empty object or diff. classes
        if (o == null || getClass() != o.getClass()) return false;
        FileHash index = (FileHash) o;
        return file == index.file && entry == index.entry;}

    @Override
    // string msg-debug
    public String toString() {return "ColumnIndex["+ file + " | " + entry + ']';}

    @Override
    public int compareTo(Object o) {
        return ((this == o) ? 0 : 1);
    }
}