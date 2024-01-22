# domain
**Lightweight SQL Persistence Layer for Java**

How it works:
1) Let all classes with objects to persist extend `SqlDomainObject` class directly or indirectly (inheritance is supported)
2) Use `Java2Sql` tool to automatically generate SQL scripts from these classes
3) Generate persistence database using these scripts
4) Configure database connection in `db.properties`
5) On programm startup create `SqlDomainController` object and call `SqlDomainController#synchronize()` to synchronize object store in heap with persistence database
6) Create objects using `DomainController#create(Class, Consumer init)` or using individuell constructors and `DomainController#register(DomainObject)`
7) Save objects in persistence database using `#save()` or create and save objects immediately with `SqlDomainController#createAndSave(Class, Consumer init)`
8) Find objects using methods like `DomainController#findAll(Class, Predicate)`, `DomainController#findAny(Class, Predicate)`

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
