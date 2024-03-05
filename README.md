# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your persistence needs, this software may be worth a look. 

***domain* supports *Oracle*, *MS/SQL-Server*, *MySQL* and *MariaDB***

How to use:
1) Let all your classes to persist extend `SqlDomainObject` class directly or indirectly (inheritance is supported)
2) Let `Java2Sql` tool generate the SQL scripts for the persistence database and generate this database
4) Configure the database connection (`db.properties`)
5) In your code:
   - Initially create a 'domain controller' and call `SqlDomainController#synchronize()` to load objects from persistence database
   - Create objects to persist using `DomainController#create()` or create objects by constructors and register them for persitence with `DomainController#register()`
   - Persist objects with `#save()` - or create and immediately persist objects using `SqlDomainController#createAndSave()`
   - Access objects using methods like `DomainController#findAll()`, `DomainController#findAny()`

Which data can be persisted?
- Basic data types are supported natively - `String`, `Integer`, `Long`, `Double` (and appropriate primitive types) - also `Enum` types, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `File`
- Lists, sets, arrays and maps of these types are also supported natively
- You can use persist also other types by defining specific conversion providers

The following topics are addressed by *Domain*:
- inheritance - there is no restriction for persistence regarding class inheritence (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- parent child relations of domain objects (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`)
- direct access to children by managed 'accumulations' fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- circular references on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- n:m relations between domain objects - using helper classes (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- protection of sensitive data - store data encrypted in database using `@Crypt` annotation and suppress logging sensitive data for all log levels using `@Secret` annotation
- house keeping - keep only relevant objects in heap (which are newer than a configurable time in the past) using `@UseDataHorizon` annotation and `dataHorizonPeriod` property  
- selective object loading - load only a part of the persisted objects using `SqlDomainController#loadOnly()`[^1]
- concurrent access - operate with multiple domain controller instances on the same persistence database, synchronize concurrent access using `SqlDomainController#allocateObjectsExclusively()`[^1]
- version control - annotate version information to new, changed and removed classes and fields and let `Java2Sql` tool automatically generate incremental database update scripts 

[^1]: SQL knowledge and knowledge of *domain* specific Java <-> SQL conversion is needed if objects shall be loaded seletively from database and if objects shall be allocated exclusively in multiple domain controller instance configurations. For such applications see Javadoc of the appropriate methods.

Also good to know:
- *domain* ensures referential integrity even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- *domain* runs in Java >=8 environments
- *domain* has a small footprint of 10k LoC and 200kB jar and needs only logging (*slf4j* and *log4j* V2) libraries and the specific database driver
