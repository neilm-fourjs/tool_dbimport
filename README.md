# tool_dbimport

A simple program to import an Informix dbexport directory with a little more control then the default dbimport command.

Create the database in the required dbspace with the required logging.

fglrun dbimport.42r mydb

On first run nothing will be dropped/created/loaded etc.
The program will only will create a file <dbname>.cfg which will be JSON and allow you to configure what happens during the next run.
