
package db61b;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.io.*;

import static db61b.Utils.*;

/** A single table in a database.
 *  @author P. N. Hilfinger
 */
class Table implements Iterable<Row> {
    /** A new Table whose columns are given by COLUMNTITLES, which may
     *  not contain dupliace names. */
    Table(String[] columnTitles) {
        for (int i = columnTitles.length - 1; i >= 1; i -= 1) {
            for (int j = i - 1; j >= 0; j -= 1) {
                if (columnTitles[i].equals(columnTitles[j])) {
                    throw error("duplicate column name: %s",
                                columnTitles[i]);
                }
            }
        }
        _columnTitles = columnTitles;
    }

    /** A new Table whose columns are give by COLUMNTITLES. */
    Table(List<String> columnTitles) {
        this(columnTitles.toArray(new String[columnTitles.size()]));
    }

    /** Return the number of columns in this table. */
    public int columns() {
        return _columnTitles.length;
    }
    /** Return the title of the Kth column.  Requires 0 <= K < columns(). */
    public String getTitle(int k) {
        if (k > columns() || k < 0) {
            throw new
            IndexOutOfBoundsException("k is either too large or too small");
        }
        return _columnTitles[k];
    }

    /** Return the number of the column whose title is TITLE, or -1 if
     *  there isn't one. */
    public int findColumn(String title) {
        for (int i = 0; i < _columnTitles.length; i++) {
            if (_columnTitles[i].equals(title)) {
                return i;
            }
        } return -1;
    }
    /** Return the number of Rows in this table. */
    public int size() {
        return _rows.size();
    }
    /** Returns an iterator that returns my rows in an unspecfied order. */
    @Override
    public Iterator<Row> iterator() {
        return _rows.iterator();
    }
    /** Add ROW to THIS if no equal row already exists.  Return true if anything
     *  was added, false otherwise. */
    public boolean add(Row row) {
        if (_rows.contains(row)) {
            return false;
        } else  {
            _rows.add(row);
            return true;
        }
    }
    /** Read the contents of the file NAME.db, and return as a Table.
     *  Format errors in the .db file cause a DBException. */
    static Table readTable(String name) {
        BufferedReader input;
        Table table;
        input = null;
        table = null;
        try {
            input = new BufferedReader(new FileReader(name + ".db"));
            String header = input.readLine();
            if (header == null) {
                throw error("missing header in DB file");
            }
            String[] columnNames = header.split(",");
            String newHeader = input.readLine();
            table = new Table(columnNames);
            while (newHeader != null) {
                String[] row = newHeader.split(",");
                Row newRow = new Row(row);
                table.add(newRow);
                newHeader = input.readLine();
            }

        } catch (FileNotFoundException e) {
            throw error("could not find %s.db", name);
        } catch (IOException e) {
            throw error("problem reading from %s.db", name);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    /* Ignore IOException */
                }
            }
        }
        return table;
    }
    /** Write the contents of TABLE into the file NAME.db. Any I/O errors
     *  cause a DBException. */
    /** Use toString() because it is of type array. */
    void writeTable(String name) {
        PrintStream output;
        output = null;
        try {
            String sep;
            sep = "";
            output = new PrintStream(name + ".db");
            for (int k = 0; k < _columnTitles.length; k++) {
                if (k != _columnTitles.length - 1) {
                    output.print(getTitle(k) + ",");
                } else {
                    output.print(getTitle(k));
                }
            } output.println();
            for (Row value: _rows) {
                for (int i = 0; i < value.size(); i++) {
                    if (i != value.size() - 1) {
                        output.print(value.get(i) + ",");
                    } else {
                        output.print(value.get(i));
                    }
                } output.println();
            }
        } catch (IOException e) {
            throw error("trouble writing to %s.db", name);
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }
    /** Print my contents on the standard output. */
    void print() {
        for (Row value: _rows) {
            System.out.print(" ");
            for (int i = 0; i < value.size(); i++) {
                System.out.print(value.get(i) + " ");
            } System.out.println();
        }
    }
    /** Return a new Table whose columns are COLUMNNAMES, selected from
     *  rows of this table that satisfy CONDITIONS. */
    Table select(List<String> columnNames, List<Condition> conditions) {
        Table result = new Table(columnNames);
        List<Column> listColumns = new ArrayList<Column>();
        Iterator iterColumnNames = columnNames.iterator();
        while (iterColumnNames.hasNext()) {
            Column c = new Column((String) iterColumnNames.next(), this);
            listColumns.add(c);
        }
        if (conditions.isEmpty()) {
            for (Row eachRow : _rows) {
                Row r = new Row(listColumns, eachRow);
                result.add(r);
            }
        } else {
            for (Row eachRow : _rows) {
                for (Condition eachConditions: conditions) {
                    if (eachConditions.test(conditions, eachRow)) {
                        Row r = new Row(listColumns, eachRow);
                        result.add(r);
                    }
                }
            }
        }
        return result;
    }
    /** Return a new Table whose columns are COLUMNNAMES, selected
     *  from pairs of rows from this table and from TABLE2 that match
     *  on all columns with identical names and satisfy CONDITIONS.*/
    Table select(Table table2, List<String> columnNames,
        List<Condition> conditions) {
        Table result = new Table(columnNames);
        List<Column> listColumnNames = new ArrayList<Column>();
        List<Column> listColumnsCommon1 = new ArrayList<Column>();
        List<Column> listColumnsCommon2 = new ArrayList<Column>();
        Iterator iterTable2 = table2.iterator();
        Row t2;
        for (int y = 0; y < columnNames.size(); y++) {
            Column c = new Column(columnNames.get(y), this, table2);
            listColumnNames.add(c);
        }
        for (int j = 0; j < this.columns(); j++) {
            String colname = getTitle(j);
            int i = table2.findColumn(colname);
            if (i >= 0) {
                Column c1 = new Column(this.getTitle(j), this);
                Column c2 = new Column(table2.getTitle(i), table2);
                listColumnsCommon1.add(c1);
                listColumnsCommon2.add(c2);
            }
        }
        while (iterTable2.hasNext()) {
            t2 = (Row) iterTable2.next();
            for (Row eachRow: _rows) {
                if (equijoin(listColumnsCommon1, listColumnsCommon2,
                    eachRow, t2) && Condition.test(conditions, eachRow, t2)) {
                    Row r = new Row(listColumnNames, eachRow, t2);
                    result.add(r);
                }
            }
        }
        return result;
    }
    /** Return true if the columns COMMON1 from ROW1 and COMMON2 from
     *  ROW2 all have identical values.  Assumes that COMMON1 and
     *  COMMON2 have the same number of elements and the same names,
     *  that the columns in COMMON1 apply to this table, those in
     *  COMMON2 to another, and that ROW1 and ROW2 come, respectively,
     *  from those tables. */
    private static boolean equijoin(List<Column> common1, List<Column> common2,
        Row row1, Row row2) {
        Iterator iterCommon1 = common1.iterator();
        Iterator iterCommon2 = common2.iterator();
        while (iterCommon1.hasNext()) {
            if (!(((Column) iterCommon1.next()).getFrom(row1)).
                equals((((Column) iterCommon2.next()).getFrom(row2)))) {
                return false;
            }
        }
        return true;
    }

    /** My rows. */
    private HashSet<Row> _rows = new HashSet<>();
    /** My columnTitles. **/
    private String[] _columnTitles;
}

