# domain
**Lightweight SQL Persistence Layer for Java**

How to use it:
1) In your code: Let all classes to persist extend `SqlDomainObject` class directly or indirectly (class inheritance is supported)
2) Let `Java2Sql` tool generate the SQL scripts for the persistence database and generate the database
4) Configure the database connection in `db.properties`
5) In your code:
   - Create an SqlDomainController` object on startup and call `SqlDomainController#synchronize()` to synchronize with persistence database
   - Create objects using `DomainController#create(Class, Consumer init)` (or create objects by constructors and register them with `DomainController#register(DomainObject)`)
   - Save objects with `#save()` or create and immediately save objects using `SqlDomainController#createAndSave(Class, Consumer init)`
   - Access objects using methods like `DomainController#findAll(Class, Predicate)`, `DomainController#findAny(Class, Predicate)`

What it supports:
- different databasess - currently *Oracle*, *MS-SQL-Server*, *MySQL* and *MariaDB*
- version control - version information can be annotated to new, changed and removed classes and fields - `Java2Sql` generates incremental database update scripts 
- class inheritance - `Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`
- protection of sensitive data - store encrypted data in database using `@Crypt` annotation and suppress logging sensitive data for all log levels using `@Secret` annotation
- house keeping - keep only relevant objects in heap (which are newer than a configurable time in the past) using `@UseDataHorizon` annotation and `dataHorizonPeriod` in `domain.properties`  
- selective object loading - load only a part of the persisted objects using `SqlDomainController#loadOnly(Class, String whereClause, int max)`[^1]
- concurrent access - operate with multiple domain controller instances on the same persistence database, synchronize access (e.g. for order processing) using `SqlDomainController#allocateObjectsExclusively()`[^1]
- `String`, `Integer`, `Long`, `Double` (and primitive types), `Enum`, `LocalDate`, `LocalTime`, `LocalDateTime`, `byte[]`, `File` and lists, sets and maps of these types as persistable data types
- Also all other types if a conversion provider for these types is defined (TODO)
- parent child relations of domain objects (`class Bike { Manufacturer manufacturer; }`)
- direct access to children by managed 'accumulations' fields (`class Manufacturer { @Accumulation Set<Bike> bikes; }`)
- circular references - on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- n:m relations between domain objects - using helper classes (`class A {}`, `class B {}`, `class AB { A a; B b; }`)

[^1]: SQL knowledge and knowledge of *domain* specific Java <-> SQL conversion is needed if objects shall be loaded seletively from database and if objects shall be allocated exclusively in multiple instance cofigurations. For such applications see Javadoc of the appropriate methods.

Also good to know:
- *domain* ensures referential integrity even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- source code of *domain* is Java 8 compatible
- *domain* has a small footprint of 10k LoC and 200kB jar and needs only logging (*slf4j* and *log4j* V2) libraries and the specific database driver
