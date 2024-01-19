# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- objects to persist ('domain objects') extend `SqlDomainObject` class
- 'domain controller' manages object store with domain objects during runtime
- domain objects can be selectively accessed using methods like 'DomainController#findAny(Class, Predicate)', 'DomainController#findAll(Class, Predicate)', etc.
- `SqlDomainController#synchronize()` initially loads objects from persistence database and subsequently synchronizes object store and database
- domain objects can individually be stored to persistence database using `SqlDomainObject#save()`
- `Java2Sql` tool analyses classes of a project and generates SQL scripts to create and update persistence database

Supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - creation, modification and/or deletion version can be annotated to new, changed and removed classes and fields - Java2Sql then automatically generates incremental database update scripts for any version 
- class inheritance - object classes may be derived from other object classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- data horizon - only objects newer than a configurable timespan in the past will be loaded and older objects will be removed from object store on synchronization
- selective object loading and referential integrity - amount of persisted objects to loaded from database can be shrinked
- referential integrity - referential integrity of loaded objects is ensured even if not all persisted objects are loaded
- circular references - on class and object level
- accumulations - fields containing collections of child objects will automatically be updated on reference changes
- multiple instances - multiple domain controller instances can act one persistence database, concurrent access can be synchronized   

Advantages:
- small footprint - < 10k LoC, few external libraries (logging and database drivers) 
- automatic generation of create and update scripts for persistence database    

