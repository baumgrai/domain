# domain
**SQL Persistence Layer for Java Objects**

Lightweight SQL persistence layer for Java objects. Currently supports Oracle, MS-SQL-Server, MySQL and MariaDB databases.

Keypoints:
- objects to persist extend `DomainObject` class
- `DomainController` class provides global persistence methods
- persisted objects can be loaded from database on startup using `DomainController.load()` and individually be saved using `DomainObject.save()`
- `DomainController.synchronize()` saves all unsaved objects to and loads potential new objects from database

Features:
- supports inheritance - e.g. `Bike extends DomainObject`, `SportiveBike extends Bike`, `Racebike extends SportiveBike`
- supports data horizon - only objects newer than a configured time in the past (data horizon) will be loaded and objects which fell out of data horizon will be removed from heap on synchronization if 'data horizon' is defined
- supports selective object loading - amount of objects to load can be restricted (by SQL WHERE clause) - referential integrity of loaded objetcts will be ensured
- supports circular references on class and object level (A.b, B.c, C.a)

Advantages:
- small footprint - depends only on logging (slf4j, log4j) and database drivers
- simple usage - small API
- less restrictions
- almost no SQL necessary (ony on selective object loading)
