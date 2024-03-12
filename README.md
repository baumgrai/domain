# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your persistence needs, this software may be worth a look. 

Supports ***Oracle*, *MS/SQL-Server*, *MySQL* and *MariaDB***

**Usage:**

Let all your 'domain' classes to persist extend `SqlDomainObject` class directly or indirectly (inheritance is supported).

Let `Java2Sql` tool generate the SQL scripts for the persistence database based on your 'domain' classes and generate the persistence database.

Configure the database connection in `db.properties`.

In your application:
   - Initially create a 'domain controller' and call `SqlDomainController#synchronize()` to load objects from persistence database
   - create and persist objects immediately using `SqlDomainController#createAndSave()`
   - if you prefer constructors: persist new objects with `SqlDomainController#save()`
   - on `#create()`, `#createAndSave()` or `#save()` objects will be registered in domain controller's object store. You may search objects there by predicates using methods like `DomainController#findAll()`, `DomainController#findAny()`
   - Remove objects from object store and delete associated persistence records from database using `#delete()`

**How data is persisted?**
- Every 'domain' class has a corresponding table in the persistence database
- Fields of type `String`, `Char`, `Short`, `Integer`, `Long`, `Double` (and appropriate primitive types) `Enum`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `char[]`, `File` have corresponding columns of appropriate type in the persistence table
- List, set, array and map fields have corresponding 'entry' tables
- Fields of any other type, for which a string conversion provider is defined, have corresponding text columns

**Topics addressed by *Domain* persistence mechanism:**
- support of class inheritance - there is no restriction regarding inheritance of domain classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- representation of parent child relations of domain objects (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`) and also of n:m relations between domain objects - using helper classes (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- direct access to object's children by managed 'accumulations' fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- support of circular references on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- protection of sensitive data - encrypt data in database using `@Crypt` annotation and suppress logging sensitive data using `@Secret` annotation
- house keeping - keep only relevant objects (which are newer than a configurable time in the past) in heap using `@UseDataHorizon` annotation and `dataHorizonPeriod` property  
- selective object loading - load only a part of the persisted objects using `SqlDomainController#loadOnly()`[^1]
- ensuring referential integrity - even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- allowing concurrent access to persistence database - operate with multiple threads and/or domain controller instances on the same persistence database, synchronize concurrent access using `SqlDomainController#allocateObjectsExclusively()`[^1]
- **version control** - annotate version information to \*new, *changed* and ~~removed~~ classes and fields and let `Java2Sql` tool automatically generate incremental database update scripts 

[^1]: SQL knowledge and knowledge of *domain* specific Java <-> SQL conversion is needed if objects shall be loaded seletively from database and if objects shall be allocated exclusively. *domain* specific Java <-> SQL conversion is described in package documentation of Javadoc.

**Further information:**
- *domain* runs in Java >=8 environments
- *domain* has a small footprint of 10k LoC and 200kB jar
- ***domain* has only logging (*slf4j* and *logback*) and database drivers as dependencies**
- Demo applications 'BikeStore' and 'Survey' and unit tests demonstrates usage of all features  
- Unit tests cover > 85% of code
