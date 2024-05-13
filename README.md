# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your persistence needs, this software may be worth a look. 

It supports ***Oracle*, *MS/SQL-Server*, *MySQL*/*MariaDB***.

**Usage:**

Let all your 'domain' classes to persist extend `SqlDomainObject` class directly or indirectly (inheritance is supported).

Let - I - `Java2Sql` tool generate the SQL scripts based on your 'domain' classes and - II - build the persistence database using these scripts.

In your application:
   - initially create an `SqlDomainController` object, which connects to the persistence database
   - call `SqlDomainController#synchronize()` to load objects from persistence database
   - create and persist objects using `SqlDomainController#createAndSave()` - or, if you prefer constructors: persist new objects with `SqlDomainController#save()`
   - with `#createAndSave()` or `#save()` objects will be registered in domain controller's object store. You may search objects there by predicates using methods like `DomainController#findAll()`, `DomainController#findAny()`
   - remove objects from object store and delete associated persistence records from database using `#delete()`

**Features:**
- supports class inheritance - there is no restriction regarding inheritance of domain classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- representation of parent child relations of domain objects (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`) and also of n:m relations (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- allows direct access to object's children by managed 'accumulation' fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- supports circular references on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- protection of sensitive data - encrypt data in database using `@Crypt` annotation and suppress logging sensitive data at any log level using `@Secret` annotation
- house keeping - keep only relevant objects (which are newer than a configurable time in the past) in heap using `@UseDataHorizon` annotation and `dataHorizonPeriod` property  
- selective object loading - load only a part of the persisted objects from database using `SqlDomainController#loadOnly()`[^1]
- ensures referential integrity - even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- allows concurrent access to persistence database - operate with multiple threads and/or domain controller instances on the same persistence database and synchronize concurrent access using `SqlDomainController#allocateObjectsExclusively()`[^1][^2]

[^1]: SQL knowledge and knowledge of *domain* specific Java <-> SQL naming conversion rules is needed only for building WHERE clauses if objects shall be loaded seletively from database or if objects shall be allocated exclusively. Java <-> SQL naming conversion rules are described in Javadoc.
[^2]: If only one domain controller instance operates on your persistence database at the same time, you can save your objects whenever you want - for performance reasons may be not until your program ends (program is master). If multiple domain controller instances operate parallely on the persistence database, objects must be saved immediately after creation or change and access to objects must be synchronized by allocating objects exclusively before reading and/or changing them (database is master) 

**Version Control:** 
- annotate version information to \*new, *changed* and ~~removed~~ domain classes and fields and let `Java2Sql` tool automatically generate incremental database update scripts 

**How data is persisted?**
- every 'domain' class has a corresponding table in the persistence database. Inherited domain classes have their own tables.
- fields of type `String`, `Char`, `Short`, `Integer`, `Long`, `Double` (and appropriate primitive types), `Enum`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `char[]`, `File` correspond to columns of appropriate type in the persistence table
- fields of any other type - for which a string conversion provider must be defined - correspond to text columns in the persistence table
- lists, sets, arrays and maps correspond to separate 'entry' tables

**Further information:**
- *domain* runs in Java >=8 environments
- *domain* has a small footprint of 10k LoC and 200kB jar
- ***domain* depends only on logging (*slf4j* and *logback*) and database drivers** (no Spring, Guava, Apache, etc. is needed)
- demo applications 'BikeStore' and 'Survey' and unit tests demonstrate usage of features  
- unit tests cover > 85% of code
