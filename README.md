# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- classes with objects to persist - 'domain classes'/'domain objects' - extend `SqlDomainObject` class
- 'domain controller' `SqlDomainController extends DomainController` manages object store with domain objects during runtime
- `SqlDomainController#synchronize()` initially loads objects and subsequently synchronizes object store and persistence database
- domain objects will individually be stored to persistence database using `SqlDomainObject#save()`
- domain objects can be accessed using methods like `DomainController#findAny(Class, Predicate)`, `#findAll(Class, Predicate)`, etc.
- `Java2Sql` tool finds domain classes of a project and generates SQL scripts to create and update persistence database

Supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - creation, modification and/or deletion version can be annotated to new, changed and removed classes and fields - Java2Sql then automatically generates incremental database update scripts for any version 
- class inheritance - object classes may be derived from other object classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- data horizon - only objects newer than a configurable timespan in the past will be loaded and older objects will be removed from object store on synchronization
- selective object loading and referential integrity - amount of persisted objects to loaded from database can be shrinked but referential integrity of loaded objects is ensured even if not all persisted objects are loaded
- circular references on class and object level
- direct access to children of domain object by managed 'accumulations' fields (`class Bike { Manufacturer manufacturer; }` `class Manufacturer { @Accumulation Set<Bike> bikes }`
- multiple domain controller instances - multiple instances can act on the same persistence database, concurrent access can be synchronized   

Advantages:
- small footprint - < 10k LoC, 200kB jar, few external libraries (only logging and database drivers) 

