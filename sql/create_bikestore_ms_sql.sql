ALTER TABLE DOM_CITY_BIKE_FEATURES DROP CONSTRAINT FK_CITY_BIKE_FEATURES#CITY_BIKE;
ALTER TABLE DOM_CITY_BIKE DROP CONSTRAINT FK_CITY_BIKE#INHERITANCE;
ALTER TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS DROP CONSTRAINT FK_RACE_BIKE_AVAILABLE_GROUP_SETS#RACE_BIKE;
ALTER TABLE DOM_RACE_BIKE DROP CONSTRAINT FK_RACE_BIKE#INHERITANCE;
ALTER TABLE DOM_MTB DROP CONSTRAINT FK_MTB#INHERITANCE;
ALTER TABLE DOM_ORDER DROP CONSTRAINT FK_ORDER#BIKE;
ALTER TABLE DOM_ORDER DROP CONSTRAINT FK_ORDER#CLIENT;
ALTER TABLE DOM_BIKE_AVAILABILITY_MAP DROP CONSTRAINT FK_BIKE_AVAILABILITY_MAP#BIKE;
ALTER TABLE DOM_BIKE_SIZES DROP CONSTRAINT FK_BIKE_SIZES#BIKE;
ALTER TABLE DOM_BIKE DROP CONSTRAINT FK_BIKE#MANUFACTURER;
ALTER TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP DROP CONSTRAINT FK_CLIENT_WANTED_BIKES_MAX_PRICE_MAP#CLIENT;

DROP TABLE DOM_CITY_BIKE_FEATURES;
DROP TABLE DOM_CITY_BIKE;
DROP TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS;
DROP TABLE DOM_RACE_BIKE;
DROP TABLE DOM_MTB;
DROP TABLE DOM_ORDER;
DROP TABLE DOM_ORDER_IN_PROGRESS;
DROP TABLE DOM_BIKE_AVAILABILITY_MAP;
DROP TABLE DOM_BIKE_SIZES;
DROP TABLE DOM_BIKE;
DROP TABLE DOM_BIKE_IN_PROGRESS;
DROP TABLE DOM_MANUFACTURER;
DROP TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP;
DROP TABLE DOM_CLIENT;
DROP TABLE DOM_CLIENT_REGION_IN_PROGRESS;

CREATE TABLE DOM_CLIENT_REGION_IN_PROGRESS
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	REGION							NVARCHAR(64),

	CONSTRAINT UNIQUE_CLIENT_REGION_IN_PROGRESS#REGION UNIQUE (REGION)
);
CREATE INDEX DOM_IDX_CLIENT_REGION_IN_PROGRESS#LAST_MODIFIED ON DOM_CLIENT_REGION_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_CLIENT
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	FIRST_NAME					NVARCHAR(512)				NOT NULL,
	GENDER							NVARCHAR(64),
	COUNTRY							NVARCHAR(64),
	BIKE_SIZE						NVARCHAR(64),
	DISPOSABLE_MONEY		FLOAT								NOT NULL,

	CONSTRAINT UNIQUE_CLIENT#NAME#COUNTRY UNIQUE (FIRST_NAME,COUNTRY)
);
CREATE INDEX DOM_IDX_CLIENT#LAST_MODIFIED ON DOM_CLIENT (LAST_MODIFIED);
CREATE INDEX DOM_IDX_CLIENT#BIKE_SIZE ON DOM_CLIENT (BIKE_SIZE);

CREATE TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP
(
	CLIENT_ID						BIGINT							NOT NULL,
	ENTRY_KEY						NVARCHAR(512),
	ENTRY_VALUE					FLOAT,

	CONSTRAINT UNIQUE_CLIENT_WANTED_BIKES_MAX_PRICE_MAP#ID#KEY UNIQUE (CLIENT_ID,ENTRY_KEY)
);

CREATE TABLE DOM_MANUFACTURER
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	NAME								NVARCHAR(512)				NOT NULL,
	COUNTRY							NVARCHAR(64),

	CONSTRAINT UNIQUE_MANUFACTURER#NAME UNIQUE (NAME)
);
CREATE INDEX DOM_IDX_MANUFACTURER#LAST_MODIFIED ON DOM_MANUFACTURER (LAST_MODIFIED);

CREATE TABLE DOM_BIKE_IN_PROGRESS
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_BIKE_IN_PROGRESS#LAST_MODIFIED ON DOM_BIKE_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_BIKE
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	MODEL								NVARCHAR(64)				NOT NULL,
	DESCRIPTION					NVARCHAR(MAX),
	FRAME								NVARCHAR(64),
	BREAKS							NVARCHAR(64),
	GEARS								INTEGER							NOT NULL,
	WEIGHT							FLOAT,
	IS_FOR_WOMAN				NVARCHAR(5)					NOT NULL,
	PRICE								FLOAT								NOT NULL,
	PICTURE							VARBINARY(MAX),
	MANUFACTURER_ID			BIGINT							NOT NULL,

	CONSTRAINT UNIQUE_BIKE#ID#MODEL UNIQUE (MANUFACTURER_ID,MODEL)
);
CREATE INDEX DOM_IDX_BIKE#LAST_MODIFIED ON DOM_BIKE (LAST_MODIFIED);

CREATE TABLE DOM_BIKE_SIZES
(
	BIKE_ID							BIGINT							NOT NULL,
	ELEMENT							NVARCHAR(64),
	ELEMENT_ORDER				BIGINT,

	CONSTRAINT UNIQUE_BIKE_SIZES#ID#ORDER UNIQUE (BIKE_ID,ELEMENT_ORDER)
);

CREATE TABLE DOM_BIKE_AVAILABILITY_MAP
(
	BIKE_ID							BIGINT							NOT NULL,
	ENTRY_KEY						NVARCHAR(64),
	ENTRY_VALUE					INTEGER,

	CONSTRAINT UNIQUE_BIKE_AVAILABILITY_MAP#ID#KEY UNIQUE (BIKE_ID,ENTRY_KEY)
);

CREATE TABLE DOM_ORDER_IN_PROGRESS
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_ORDER_IN_PROGRESS#LAST_MODIFIED ON DOM_ORDER_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_ORDER
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	WAS_CANCELED				NVARCHAR(5)					NOT NULL,
	ORDER_DATE					DATETIME,
	INVOICE_DATE				DATETIME,
	PAY_DATE						DATETIME,
	DELIVERY_DATE				DATETIME,
	CLIENT_ID						BIGINT							NOT NULL,
	BIKE_ID							BIGINT							NOT NULL
);
CREATE INDEX DOM_IDX_ORDER#LAST_MODIFIED ON DOM_ORDER (LAST_MODIFIED);

CREATE TABLE DOM_MTB
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	BIKE_TYPE						NVARCHAR(64),
	SUSPENSION					NVARCHAR(64),
	WHEEL_SIZE					NVARCHAR(64)
);

CREATE TABLE DOM_RACE_BIKE
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	IS_AERO							NVARCHAR(5)					NOT NULL,
	IS_FRAME_ONLY				NVARCHAR(5)					NOT NULL,
	IS_GEAR_SHIFT_ELECTRIC		NVARCHAR(5)					NOT NULL,
	RIM 								NVARCHAR(64)
);

CREATE TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS
(
	RACE_BIKE_ID				BIGINT							NOT NULL,
	ELEMENT							NVARCHAR(64),

	CONSTRAINT UNIQUE_RACE_BIKE_AVAILABLE_GROUP_SETS#ID#ELEMENT UNIQUE (RACE_BIKE_ID,ELEMENT)
);

CREATE TABLE DOM_CITY_BIKE
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY
);

CREATE TABLE DOM_CITY_BIKE_FEATURES
(
	CITY_BIKE_ID				BIGINT							NOT NULL,
	ELEMENT							NVARCHAR(64),

	CONSTRAINT UNIQUE_CITY_BIKE_FEATURES#ID#ELEMENT UNIQUE (CITY_BIKE_ID,ELEMENT)
);


ALTER TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP ADD CONSTRAINT FK_CLIENT_WANTED_BIKES_MAX_PRICE_MAP#CLIENT FOREIGN KEY (CLIENT_ID) REFERENCES DOM_CLIENT(ID);
ALTER TABLE DOM_BIKE ADD CONSTRAINT FK_BIKE#MANUFACTURER FOREIGN KEY (MANUFACTURER_ID) REFERENCES DOM_MANUFACTURER(ID);
ALTER TABLE DOM_BIKE_SIZES ADD CONSTRAINT FK_BIKE_SIZES#BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID);
ALTER TABLE DOM_BIKE_AVAILABILITY_MAP ADD CONSTRAINT FK_BIKE_AVAILABILITY_MAP#BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID);
ALTER TABLE DOM_ORDER ADD CONSTRAINT FK_ORDER#CLIENT FOREIGN KEY (CLIENT_ID) REFERENCES DOM_CLIENT(ID) ON DELETE CASCADE;
ALTER TABLE DOM_ORDER ADD CONSTRAINT FK_ORDER#BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_MTB ADD CONSTRAINT FK_MTB#INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_RACE_BIKE ADD CONSTRAINT FK_RACE_BIKE#INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS ADD CONSTRAINT FK_RACE_BIKE_AVAILABLE_GROUP_SETS#RACE_BIKE FOREIGN KEY (RACE_BIKE_ID) REFERENCES DOM_RACE_BIKE(ID);
ALTER TABLE DOM_CITY_BIKE ADD CONSTRAINT FK_CITY_BIKE#INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_CITY_BIKE_FEATURES ADD CONSTRAINT FK_CITY_BIKE_FEATURES#CITY_BIKE FOREIGN KEY (CITY_BIKE_ID) REFERENCES DOM_CITY_BIKE(ID);
