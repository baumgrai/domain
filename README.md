# domain
**Lightweight SQL Persistence Layer for Java**

If you - for any reason - do not want to use Hibernate, Spring, etc. for your Java persistence needs, this software could be worth a look! It supports ***Oracle*, *MS/SQL-Server*** and ***MySQL* / *MariaDB***.

Start with the **generation of the persistence database**:
1. Let all your Java classes to persist - called *domain* classes - extend `SqlDomainObject` class [^1]. 
2. Let `Java2Sql` tool generate SQL scripts (based on these domain classes) and build persistence database using these scripts.
3. Configure database connection in `db.properties` file.

At runtime **load and persist objects**:
- Initially create `SqlDomainController` object, which connects to the persistence database, and load persisted objects using `SqlDomainController#synchronize()`.
- Create and immediately save objects using `SqlDomainController#createAndSave()` or save new and changed objects with `#save()` (objects will be registered in *object store* on initial saving).
- Remove objects from object store and delete associated persistence records from database using `#delete()`.
- Search for objects in object store using `DomainController#findAll(<predicate>)`, `DomainController#findAny(<predicate>)`, etc.

Use **version control** for long-time application development: 
- Version information can be annotated to \*new, *changed* and ~~removed~~ domain classes and fields; `Java2Sql` tool then automatically generates incremental database update scripts in addition to full database generation scripts.

***domain*** persitence layer offers the following **features**:
- it supports **class inheritance** - there is no restriction regarding inheritance of domain classes (e.g.: `Bike extends SqlDomainObject`, `RaceBike extends Bike`, `Bianchi extends RaceBike`) [^1]
- it represents **parent/child relations** between domain objects in database (`class Manufacturer {...}`, `class Bike { Manufacturer manufacturer; ...}`) and may also represent n:m relations using helper classes (`class A {...}`, `class B {...}`, `class AB { A a; B b; }`)
- it allows **direct access to children** by managed *accumulation* fields (`class Manufacturer {... @Accumulation Set<Bike> bikes; }`)
- it supports **circular references** on class and object level (`class X { X next; }`, `class A { B b; }`, `class B { C c; }`, `class C { A a; }`)
- it protects **sensitive data**: you can force encryption of data in database using `@Crypt` annotation, and you also can suppress logging of sensitive data at any log level using `@Secret` annotation [^2]
- it supports house keeping by **data horizon** mechanism: only objects, which were created or changed after a configurable time horizon in the past, will be loaded, and objects running out of time will be removed from object store on `SqlDomainController#synchronize()` (this behavior is controlled by `@UseDataHorizon` class annotation and `dataHorizonPeriod` property)
- it supports **selective object loading**: you can load only a part of the persisted objects using `SqlDomainController#loadOnly()` [^3]
- it ensures **referential integrity** - even if not all persisted objects are loaded: parent is loaded if child is loaded
- it allows synchronization of **concurrent write access** to persistence database: one persistence database can be accessed by multiple domain controller instances and one controller instance can host multiple threads parallely accessing domain objects. Concurrent write access to domain objects can be synchronized using `SqlDomainController#allocateObjectsExclusively()` [^3][^4][^5]

[^1]: On inheritance the base class of the inheritance stack extends `SqlDomainObject`
[^2]: On INFO log level no object data will be logged at all. 
[^3]: Knowledge of SQL and *domain* specific Java -> SQL naming rules is needed (only) for building WHERE clauses, if objects shall be loaded selectively from database or if objects shall be allocated exclusively. Java -> SQL naming rules are described in Javadoc.
[^4]: If only one domain controller instance operates on a persistence database, program is master - persisted objects then can be loaded from database once initially, and new or changed objects can be saved whenever wanted before program termination. If multiple domain controller instances operate parallely on the same persistence database, database is master - objects should be saved immediately after creation or change to expose changes to other instances and `SqlDomainController#synchronize()` must be called periodically by all instances to reflect changes in database made by other instances. Write access must be synchronized by allocating objects exclusively.
[^5]: If multiple domain controller instances operate parallely on the same persistence database, exclusive access to objects is synchronized on database level by so called *in-progress* records, which are uniquely associated with excusively allocated objects. This means, concurrent access synchonization for multiple instances bases on UNIQUE constraint mechanism and not on ~~SELECT FOR UPDATE~~ clause (which is never been used within *domain* persistence layer).

**How will objects be persisted?**
- Every *domain* class is associated with one database table, and every object of this class corresponds to one persistence record having a unique, auto-generated id. On inheritance every inherited class has it's own table, so one domain object has multiple database records in this case.
- Fields of types `Char`, `Short`, `Integer`, `Long`, `Double` (and primitives), `String`, `Enum`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`, `byte[]`, `char[]` and `File` correspond to columns of appropriate, database specific types.
- List, set, array and map fields correspond to separate *entry* tables, which persist collection elements or map entries and - for lists and arrays - their order.
- For fields of any other type a string conversion provider must be defined. These fields then correspond to text columns.

**Also good to know:**
- *domain* runs in Java >= 8 environments and has a small footprint of about 10k LoC and 200kB jar
- ***domain* depends only on logging (*slf4j* + *logback*) and specific database drivers** (no Spring, Guava, Apache, etc. is needed)
- unit tests and demo applications *BikeStore* and *Survey* demonstrate usage and features  
- unit tests cover > 85% of code
