# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your Java persistence needs, this software could be worth a look. 

It supports ***Oracle*, *MS/SQL-Server*** and ***MySQL* / *MariaDB***.

**Prepare and generate persistence database:**

1. Let all your Java classes to persist, called *domain classes*, extend `SqlDomainObject` class [^1]. 
2. Let `Java2Sql` tool generate SQL scripts for persistence database based on these domain classes and build persistence database using these scripts.
3. Configure database connection in `db.properties` file

**Load and persist objects at runtime:**

- Initially create `SqlDomainController` object, which connects to the persistence database, and load persisted objects using `SqlDomainController#synchronize()`.
- Create and immediately save objects using `SqlDomainController#createAndSave()` or save new and changed objects with `#save()` (objects will be registered in *object store* on initial saving).
- Remove objects from object store and delete associated persistence records from database using `#delete()`.
- Search for objects in object store using `DomainController#findAll(<predicate>)`, `DomainController#findAny(<predicate>)`, etc.

**Features:**
- supports **class inheritance** - there is no restriction regarding inheritance of domain classes (`Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`)
- represents **parent child relations** of domain objects in database (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`) and also of n:m relations (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- allows **direct access to children** by managed *accumulation* fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- supports **circular references** on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- **protects sensitive data**: encrypt data in database using `@Crypt` annotation and suppress logging of sensitive data at any log level using `@Secret` annotation
- supports house keeping by **data horizon**: only objects, which are where created or changed after a configurable time horizon in the past, will initially be loaded into and and kept in local object store. Data running out of time will be removed from object store on `SqlDomainController#synchronize()` (this behavior is controlled by `@UseDataHorizon` class annotation and `dataHorizonPeriod` property)
- supports **selective object loading**: only a part of the persisted objects will be loaded from database using `SqlDomainController#loadOnly()`[^2]
- ensures **referential integrity** - even if not all persisted objects are loaded into object store: parent is loaded if child is loaded
- allows **concurrent access** to persistence database: persistence database can be accessed by multiple threads and/or multiple domain controller instances. Concurrent access can be synchronized using `SqlDomainController#allocateObjectsExclusively()`[^2][^3]

[^1]: On inheritance the base class of the inheritance stack extends `SqlDomainObject`
[^2]: Knowledge of SQL and *domain* specific Java -> SQL naming rules is needed (only) for building WHERE clauses if objects shall be loaded selectively from database or if objects shall be allocated exclusively. Java -> SQL naming rules are described in Javadoc.
[^3]: If only one domain controller instance operates on your persistence database, you may load persisted objects from database once and save your (new or changed) objects whenever you want (program is master). If multiple domain controller instances operate parallely on one persistence database, objects must be saved immediately after creation or change and access to objects must be synchronized by allocating objects exclusively before reading and/or changing them (database is master). 

**Version Control:** 
- If version information for \*new, *changed* and ~~removed~~ domain classes and fields are annotated, `Java2Sql` tool automatically generates incremental database update scripts in addition to full database generation scripts.

**How data is persisted?**
- Every *domain* class is associated with one database table (on inheritance every class has it's own table).
- Fields of types `Char`, `Short`, `Integer`, `Long`, `Double` (and primitives), `String`, `Enum`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `char[]` and `File` correspond to columns of appropriate types.
- List, set, array and map fields correspond to separate *entry* tables, which persists entries and - if necessary - entry order.
- Fields of any other type - for which a string conversion provider must be defined - correspond to text columns.

**Further information:**
- *domain* runs in Java >=8 environments
- *domain* has a small footprint of about 10k LoC and 200kB jar
- ***domain* depends only on logging (*slf4j* + *logback*) and specific database drivers** (no Spring, Guava, Apache, etc. is needed)
- demo applications 'BikeStore' and 'Survey' and unit tests demonstrate usage  
- unit tests cover > 85% of code
