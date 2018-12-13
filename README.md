This is a rough demonstration of some code that integrates ABL with a SQL server and allows one to sync data that resides in Progress to an external table on SQL server.

To configure, change the `SCOPED-DEFINE` variables a the top of `Sequel.p` to match your environment.

The `constants.i` file is meant for use with Apprise and contains some example constants used in Apprise queries.

The `demo.p` file shows how to create and populate a temp table in ABL, then use the library to connect to a SQL server for data synchronization.