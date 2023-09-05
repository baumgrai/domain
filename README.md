# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- objects to persist extend `DomainObject` class
- `DomainController` class provides global persistence methods
- persisted objects can be loaded from database using `DomainController.load()` and individually be saved using `DomainObject.save()`
- `DomainController.synchronize()` saves all unsaved objects to and loads potential new objects from database
- `Java2Sql` tool analyses classes of a project and generates SQL scripts to create or update<sup>*</sup> persistence database

<sup>\* if version info is annotated on new or changed classes and fields</sup>

Supports:
- Oracle, MS-SQL-Server, MySQL and MariaDB databases
- class inheritance (`TwoWheeler extends DomainObject`, `Bicycle extends TwoWheeler`, `Racebike extends Bicycle`)
- data horizon - only objects newer than a configured time in the past (data horizon) will be loaded and objects which fell out of data horizon will be removed from heap on synchronization if 'data horizon' is defined
- selective object loading - not all persisted objects must be loaded; amount of objects to load can be shrinked by SQL WHERE clause
- referential integrity - for both 'data horizon' controlled and 'selective' object loading referential integrity of loaded objects is ensured
- circular references on class and object level

Advantages:
- small footprint - needs few external libraries: only for logging (slf4j, log4j) and database drivers 
- automatic generation of database generation and update scripts    
- almost no SQL necessary (only on selective object loading)
