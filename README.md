# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
- _domain_ classes with objects to persist (_domain_ objects) extend `SqlDomainObject` class
- `Java2Sql` tool generates SQL scripts to create persistence database from domain classes 
- _domain controller_ `SqlDomainController extends DomainController` manages domain object store during runtime
- `SqlDomainController#synchronize()` initially loads objects and subsequently synchronizes object store and persistence database
- domain objects can be created using `DomainController#create(Class, Consumer init)` or using constructors and `DomainController#register(DomainObject)`
- domain objects will be stored to persistence database using `#save()` or `#createAndSave()`
- domain objects can be accessed using methods like `DomainController#findAll(Class, Predicate)`

What it supports:
- different databases - Oracle, MS-SQL-Server, MySQL and MariaDB
- version control - version information can be annotated to new, changed and removed classes and fields - `Java2Sql` generates incremental database update scripts for all versions 
- class inheritance - `Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`
- parent child relations between domain objects - `class Bike { Manufacturer manufacturer; }`
- direct access to children by managed 'accumulations' fields - `class Manufacturer { @Accumulation Set<Bike> bikes; }`
- _data horizon_ - only objects newer than a configurable time in the past will be loaded and older objects will be removed from object store on synchronization
- selective object loading - not all objects in persistence database must be loaded - `SqlDomainController#loadOnly()`
- circular references on class and object level
- concurrent access - multiple domain controller instances can operate on the same persistence database, concurrent access can be synchronized using methods like `SqlDomainController#allocateObjectsExclusively()`
- Java types `String`, `Integer`, `Long`, `Double` (and primitive types), `Enum`, `LocalDate`, `LocalTime`, `LocalDateTime`, `byte[]`, `File` for persistable fields of domain classes
- Also all other types if a conversion provider for these types is defined (TODO)
- `List`s, `Set`s and `Map`s of these types (`List<Type>` for `enum Type`) as persistable field types and as elements of collections or values of maps (`Map<String>, Set<Integer>`, `List<Map<LocalDate, Integer>>`)

Further information:
- _domain_ esures referential integrity - means parent is loaded if child is loaded - of loaded objects even if not all objects are loaded from persistence database
- the only applications where SQL knowledge and knowledge about _domain_ specific Java <-> SQL conversion is needed, are selective object loading and allocating object exclusively, all others are Java-only
- _domain_ has a small footprint: 10k LoC, 200kB jar and few external libraries - only logging (slf4j2) and database drivers
