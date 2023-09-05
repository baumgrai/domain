# domain
**Lightweight SQL Persistence Layer for Java Objects**

How it works:
- objects to persist extend `DomainObject` class
- `DomainController` class provides global persistence methods
- persisted objects can be loaded from database using `DomainController.load()` and individually be saved using `DomainObject.save()`
- `DomainController.synchronize()` saves all unsaved objects to and loads potential new objects from database
- `Java2Sql` tool analyses classes of a project and generates SQL scripts to create or update persistence database

Supports:
- Oracle, MS-SQL-Server, MySQL and MariaDB databases
- inheritance (`TwoWheeler extends DomainObject`, `Bicycle extends TwoWheeler`, `Racebike extends Bicycle`)
- data horizon - only objects newer than a configured time in the past (data horizon) will be loaded and objects which fell out of data horizon will be removed from heap on synchronization if 'data horizon' is defined
- selective object loading - loading objects may be selective
- for both 'data horizon' and 'selective object loading' referential integrity of loaded objetcts will be ensured
- circular references on class and object level

Advantages:
- small footprint - needs external libraries only for logging (slf4j, log4j) and database drivers 
- automatic generation of persistence database from Java classes
- allows generating update scripts for database on class changes
- almost no SQL necessary (only on selective object loading)
