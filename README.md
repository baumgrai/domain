# domain
**Lightweight SQL Persistence Layer for Java**

How to use it:
1) In your code: Let all classes to persist extend `SqlDomainObject` class directly or indirectly (class inheritance is supported)
2) Generate SQL scripts for persistence database using `Java2Sql` tool  and generate persistence database
4) Configure database connection in `db.properties`
5) In your code:
   - Create `SqlDomainController` object on startup and call `SqlDomainController#synchronize()` to synchronize with persistence database
   - Create and register *domain* objects using `DomainController#create(Class, Consumer init)` or using individuell constructors and `DomainController#register(DomainObject)`
   - Save domain objects using `#save()` or create and save objects immediately with `SqlDomainController#createAndSave(Class, Consumer init)`

What it supports:
- currently *Oracle*, *MS-SQL-Server*, *MySQL* and *MariaDB*
- version control - version information can be annotated to new, changed and removed classes and fields - `Java2Sql` generates incremental database update scripts for consecutive versions 
- inheritance - `Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`
- encrypting data in database using @Crypt annotation, suppressing of logging data for all log levels using @Secret annotation
- circular references on class and object level - `class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`
- "NoSQL" selection - objects can be selected from object store using methods like `DomainController#findAll(Class, Predicate)`, `DomainController#findAny(Class, Predicate)` which do not need SQL where clauses
- *data horizon* - only objects newer than a configurable time in the past (`dataHorizonPeriod` in `domain.properties`) will be loaded on synchronization, and older objects will be removed from object store for classes where `@UseDataHorizon` is annotated 
- selective object loading - not all objects in persistence database must be loaded - `SqlDomainController#loadOnly(Class, String whereClause, int max)`[^1]
- concurrent access - multiple domain controller instances can operate on the same persistence database, access synchronization (e.g. for order processing) can be made using `SqlDomainController#allocateObjectsExclusively()`[^1]
- `String`, `Integer`, `Long`, `Double` (and primitive types), `Enum`, `LocalDate`, `LocalTime`, `LocalDateTime`, `byte[]`, `File` as allowed Java types for persistable fields of domain classes
- Also all other types if a conversion provider for these types is defined (TODO)
- `List`s, `Set`s and `Map`s of these types - `List<String>`, `Map<Type, LocalDateTime>` (where `Type` is an enum) - also as elements of collections or values of maps - `List<Map<LocalDate, Double>>`, `Map<String, Set<Integer>>`
- parent child relations between domain objects - `class Bike { Manufacturer manufacturer; }` - and direct access to children by managed 'accumulations' fields - `class Manufacturer { @Accumulation Set<Bike> bikes; }`
- n:m relations between domain objects - using helper classes - `class A {}`, `class B {}`, `class AB { A a; B b; }`

[^1]: The only applications where SQL knowledge (and knowledge about *domain* specific Java <-> SQL conversion) is needed, are selective object loading and allocating objects exclusively, all others are Java-only. See Javadoc of the methods used for these applications.

Also good to know:
- *domain* ensures referential integrity even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- source code of *domain* is Java 8 compatible
- *domain* has a small footprint (10k LoC, 200kB jar), and has only logging (*slf4j* and *log4j* V2) libraries and specific database driver as external dependencies
