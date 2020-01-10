csv_timeline_condense.py takes CSV or similar formatted data containing timestamps and condenses the dataset. This is done by specifying a key, which can be one or more columns, containing the column value(s) you want tracked. The condensed output will contain the unique key column(s) along with the date first seen, date last seen, and the count of rows removed. You can also choose between outputing the first or last row identified to provide further example of the data. 

The csv_timeline_condense.py script can be handy when reviewing logs from firewalls, Windows Events, or any data export that contains a large number of entries with timestamps. For data that does not contain timestamps please use csv_condense.py instead.