# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- classes with objects to persist - _domain classes/domain objects_ - extend `SqlDomainObject` class
- `Java2Sql` tool finds domain classes of a project and generates SQL scripts to create persistence database
- _domain controller_ `SqlDomainController extends DomainController` manages domain object store during runtime
- `SqlDomainController#synchronize()` initially loads objects and subsequently synchronizes object store and persistence database
- domain objects can be created using `DomainController#create()` or using constructors and `DomainController#register(DomainObject)`
- domain objects will be stored to persistence database using `#save()`
- domain objects can be accessed using methods like `DomainController#findAll(Class, Predicate)`

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

