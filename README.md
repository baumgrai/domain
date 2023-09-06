# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- objects to persist extend `SqlDomainObject` class
- persisted objects can be loaded from database using `SqlDomainController.load()` and individually be saved using `SqlDomainObject.save()`
- `SqlDomainController.synchronize()` saves all unsaved objects to and loads potential new objects from database
- `Java2Sql` tool analyses classes of a project and generates SQL scripts to create or update persistence database

Supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - incremental database update scripts will be generated automatically for new and changed fields and classes
- class inheritance - object classes may be derived (`Vehicle extends SqlDomainObject`, `Car extends Vehicle`, `Sportscar extends Car`)
- data horizon - only new objects will be loaded and old objects will be removed from heap on synchronization
- selective object loading - amount of persisted objects to load can be shrinked (TODO)
- referential integrity - referential integrity of loaded objects is ensured even if not all persisted objects are loaded
- circular references - on class and object level

Advantages:
- small footprint - < 10k LoC, few external libraries (logging and database drivers) 
- automatic generation of create and update scripts for persistence database    

