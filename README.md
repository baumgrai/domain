# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- classes with objects to persist - _domain classes/domain objects_ - extend `SqlDomainObject` class
- `Java2Sql` tool finds domain classes of a project and generates SQL scripts to create persistence database
- _domain controller_ `SqlDomainController extends DomainController` manages domain object store during runtime
- `SqlDomainController#synchronize()` initially loads objects and subsequently synchronizes object store and persistence database
- domain objects can be created using `DomainController#create()` or using constructors and `DomainController#register(DomainObject)`
- domain objects will be stored to persistence database using `#save()` or `#createAndSave()`
- domain objects can be accessed using methods like `DomainController#findAll(Class, Predicate)`

What it supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - creation, modification and/or deletion version can be annotated to new, changed and removed classes and fields - `Java2Sql` then automatically generates incremental database update scripts for all versions 
- class inheritance - object classes may be derived from other object classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- _data horizon_ - only objects newer than a configurable time in the past will be loaded and older objects will be removed from object store on synchronization
- selective object loading and referential integrity - amount of persisted objects to load from database can be shrinked
- referential integrity of loaded objects is ensured even if not all persisted objects are loaded (due to data horizon or selectibe object loading)
- circular references on class and object level
- direct access to current children of a domain object by managed 'accumulations' fields (`class Bike { Manufacturer manufacturer; }` `class Manufacturer { @Accumulation Set<Bike> bikes; }`
- concurrent access - multiple domain controller instances can operate on the same persistence database, concurrent access can be synchronized using methods like `allocateObjectsExclusively()`
- Java types `String`, `Integer`, `Long`, `Double` (and primitive types), `enum`, `LocalDate`, `LocalTime`, `LocalDateTime`, `byte[]`, `File` for persistable fields of domain classes
- All other types if a conversion provider for these types is defined (TODO)
- `List`s, `Set`s and `Map`s of these types (`List<Type>` for `enum Type`) as persistable field types
- `List`s, `Set`s and `Map`s as elements of collections or values of maps (`Map<String>, List<Integer>`, `Set<Map<LocalDate, Integer>>`)  

Further information:
- _domain_ has a small footprint: 10k LoC, 200kB jar and few external libraries - only logging (slf4j2) and database drivers
