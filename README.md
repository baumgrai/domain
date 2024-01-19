# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- objects to persist extend `SqlDomainObject` class
- `SqlDomainController.synchronize()` initially loads objects from database and synchronizes local object store with persistence database
- persisted objects can individually be saved using `SqlDomainObject.save()`
- `Java2Sql` tool analyses classes of a project and generates SQL scripts to create or update persistence database

Supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - creation, modification and/or deletion version can be annotated to new, changed and removed classes and fields - Java2Sql then automatically generates incremental database update scripts for any version 
- class inheritance - object classes may be derived (`Vehicle extends SqlDomainObject`, `Car extends Vehicle`, `Sportscar extends Car`)
- data horizon - only new objects will be loaded and old objects will be removed from heap on synchronization
- selective object loading - amount of persisted objects to load can be shrinked (TODO)
- referential integrity - referential integrity of loaded objects is ensured even if not all persisted objects are loaded
- circular references - on class and object level
- accumulations - fields containing collections of child objects will automatically be updated on reference changes

Advantages:
- small footprint - < 10k LoC, few external libraries (logging and database drivers) 
- automatic generation of create and update scripts for persistence database    

