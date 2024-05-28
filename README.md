# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your Java persistence needs, this software may be worth a look. 

It supports ***Oracle*, *MS/SQL-Server*, *MySQL* / *MariaDB***.

**Usage:**

- Let all your *domain* classes to persist extend `SqlDomainObject` class (directly or indirectly - inheritance is supported).
- Create `SqlDomainController` object which connects to the persistence database and initially load persisted objects using `SqlDomainController#synchronize()`.
- Create and persist objects using `SqlDomainController#createAndSave()` or persist new and changed objects with `#save()`.
- With `#createAndSave()` or `#save()` objects will automatically be registered in domain controller's *object store*. Search objects there by predicates using `DomainController#findAll(...)`, `DomainController#findAny(...)`, etc.
- Remove objects from object store and delete associated persistence records from database using `#delete()`.

**Generate persistence database:**
1. Let `Java2Sql` tool generate SQL scripts based on your *domain* classes to persist.
2. Build the persistence database using these scripts.

**Features:**
- supports **class inheritance** - there is no restriction regarding inheritance of domain classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- representation of **parent child relations** of domain objects (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`) and also of n:m relations (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- allows **direct access to children** by managed 'accumulation' fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- supports **circular references** on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- **protection of sensitive data**: encrypt data in database using `@Crypt` annotation and suppress logging of sensitive data at any log level using `@Secret` annotation
- house keeping: **keep only relevant objects in heap** (which are newer than a configurable time in the past) using `@UseDataHorizon` annotation and `dataHorizonPeriod` property  
- **selective object loading**: load only a part of the persisted objects from database using `SqlDomainController#loadOnly()`[^1]
- ensures **referential integrity** - even if not all persisted objects are loaded into object store - parent is loaded if child is loaded
- allows **concurrent access** to persistence database: access persistence database by multiple threads and/or domain controller instances and synchronize concurrency using `SqlDomainController#allocateObjectsExclusively()`[^1][^2]

[^1]: knowledge of SQL and *domain* specific Java <-> SQL naming conversion rules is needed (only) for building WHERE clauses if objects shall be loaded selectively from database or if objects shall be allocated exclusively. Java <-> SQL naming conversion rules are described in Javadoc.
[^2]: If only one domain controller instance operates on your persistence database, it is sufficiant to initially load persisted objects from database (`#synchronize`), and you may save your objects whenever you want (program is master). If multiple domain controller instances operate parallely on one persistence database, objects must be saved immediately after creation or change, and access to objects must be synchronized by allocating objects exclusively before reading and/or changing them (database is master). 

**Version Control:** 
- annotate version information to \*new, *changed* and ~~removed~~ domain classes and fields and let `Java2Sql` tool automatically generate incremental database update scripts 

**How data is persisted?**
- every *domain* class has a corresponding table in the persistence database (inherited domain classes have their own tables).
- fields of type `String`, `Char`, `Short`, `Integer`, `Long`, `Double` (and appropriate primitive types), `Enum`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `char[]`, `File` correspond to columns of appropriate type in the persistence tables
- fields of any other type - for which a string conversion provider must be defined - correspond to text columns in the persistence table
- lists, sets, arrays and maps correspond to separate *entry* tables

**Further information:**
- *domain* runs in Java >=8 environments
- *domain* has a small footprint of about 10k LoC and 200kB jar
- ***domain* depends only on logging (*slf4j* and *logback*) and specific database drivers** (no Spring, Guava, Apache, etc. is needed)
- demo applications 'BikeStore' and 'Survey' and unit tests demonstrate usage of features  
- unit tests cover > 85% of code
