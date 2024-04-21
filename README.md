# DBSync4 - SQL Database Schema Sync Utility

This utility is designed to synchronize the schema between two SQL databases.
It does this by reading in the schema for two databases, discovering the differences,
and creating a script to update one database schema to be the same as the other.

Here is an example of how it is used.  Let's say you have a production system and
a development system - each with their own database.  At the point of deployment 
both schemas are the same.  Then, as you develop the system further, you make schema 
changes to the development database.  When you're ready to deploy you need a script to 
update the production system to be the same as your development system.  DBSync 4 
creates the needed SQL update script by reading the schema of both and generating
the SQL update script.

## More Details

The reason DBSync is called DBSync4 is that it is the fourth version of this utility.

It is written to support PostgreSQL and Microsoft SQL Server.  In practice, over the last 
ten years, it has only been used with PostgreSQL.  It has been used in a production environment 
for more than ten years.

The text change script it generates consists of three sections as follows:

1. Remove all indexes and foreign keys
2. Make the needed schema changes
3. RE-add all the indexes and foreign keys

In general, I delete the first and third sections and just use the second.  However,
be careful, sometimes new foreign keys or indexes only appear in the third section and must be
copied to the second section before deleting the third.

Run ```build.sh``` to build.

You may want to take a look at [Stack360](https://github.com/blakemcbride/Stack360-Backend)
It contains code to perform automatic schema updates when production systems are updated.

Home for this project is [https://github.com/blakemcbride/DBSync4](https://github.com/blakemcbride/DBSync4)

