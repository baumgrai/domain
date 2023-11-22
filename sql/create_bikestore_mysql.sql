ALTER TABLE DOM_MTB DROP FOREIGN KEY FK_MTB$INHERITANCE;
ALTER TABLE DOM_CITY_BIKE_FEATURES DROP FOREIGN KEY FK_CITY_BIKE_FEATURES$CITY_BIKE;
ALTER TABLE DOM_CITY_BIKE DROP FOREIGN KEY FK_CITY_BIKE$INHERITANCE;
ALTER TABLE DOM_ORDER DROP FOREIGN KEY FK_ORDER$BIKE;
ALTER TABLE DOM_ORDER DROP FOREIGN KEY FK_ORDER$CLIENT;
ALTER TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP DROP FOREIGN KEY FK_CLIENT_WANTED_BIKES_MAX_PRICE_MAP$CLIENT;
ALTER TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS DROP FOREIGN KEY FK_RACE_BIKE_AVAILABLE_GROUP_SETS$RACE_BIKE;
ALTER TABLE DOM_RACE_BIKE DROP FOREIGN KEY FK_RACE_BIKE$INHERITANCE;
ALTER TABLE DOM_BIKE_AVAILABILITY_MAP DROP FOREIGN KEY FK_BIKE_AVAILABILITY_MAP$BIKE;
ALTER TABLE DOM_BIKE_SIZES DROP FOREIGN KEY FK_BIKE_SIZES$BIKE;
ALTER TABLE DOM_BIKE DROP FOREIGN KEY FK_BIKE$MANUFACTURER;

DROP TABLE DOM_MTB;
DROP TABLE DOM_CITY_BIKE_FEATURES;
DROP TABLE DOM_CITY_BIKE;
DROP TABLE DOM_ORDER;
DROP TABLE DOM_ORDER_IN_PROGRESS;
DROP TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP;
DROP TABLE DOM_CLIENT;
DROP TABLE DOM_CLIENT_REGION_IN_PROGRESS;
DROP TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS;
DROP TABLE DOM_RACE_BIKE;
DROP TABLE DOM_BIKE_AVAILABILITY_MAP;
DROP TABLE DOM_BIKE_SIZES;
DROP TABLE DOM_BIKE;
DROP TABLE DOM_BIKE_IN_PROGRESS;
DROP TABLE DOM_MANUFACTURER;

CREATE TABLE DOM_MANUFACTURER
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	NAME								VARCHAR(256) CHARACTER SET UTF8MB4		NOT NULL,
	COUNTRY							VARCHAR(64) CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_MANUFACTURER$NAME UNIQUE (NAME)
);
CREATE INDEX DOM_IDX_MANUFACTURER$LAST_MODIFIED ON DOM_MANUFACTURER (LAST_MODIFIED);

CREATE TABLE DOM_BIKE_IN_PROGRESS
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_BIKE_IN_PROGRESS$LAST_MODIFIED ON DOM_BIKE_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_BIKE
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	MODEL								VARCHAR(64) CHARACTER SET UTF8MB4		NOT NULL,
	DESCRIPTION					LONGTEXT CHARACTER SET UTF8MB4,
	FRAME								VARCHAR(64) CHARACTER SET UTF8MB4,
	BREAKS							VARCHAR(64) CHARACTER SET UTF8MB4,
	GEARS								INTEGER							NOT NULL,
	WEIGHT							DOUBLE,
	IS_FOR_WOMAN				VARCHAR(5)					NOT NULL,
	PRICE								DOUBLE							NOT NULL,
	PICTURE							LONGBLOB,
	MANUFACTURER_ID			BIGINT							NOT NULL,

	CONSTRAINT UNIQUE_BIKE$MANUFACTURER$MODEL UNIQUE (MANUFACTURER_ID,MODEL)
);
CREATE INDEX DOM_IDX_BIKE$LAST_MODIFIED ON DOM_BIKE (LAST_MODIFIED);

CREATE TABLE DOM_BIKE_SIZES
(
	BIKE_ID							BIGINT							NOT NULL,
	ELEMENT							VARCHAR(64) CHARACTER SET UTF8MB4,
	ELEMENT_ORDER				INTEGER
);

CREATE TABLE DOM_BIKE_AVAILABILITY_MAP
(
	BIKE_ID							BIGINT							NOT NULL,
	ENTRY_KEY						VARCHAR(64) CHARACTER SET UTF8MB4,
	ENTRY_VALUE					INTEGER,

	CONSTRAINT UNIQUE_BIKE_AVAILABILITY_MAP$BIKE$ENTRY UNIQUE (BIKE_ID,ENTRY_KEY)
);

CREATE TABLE DOM_RACE_BIKE
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	IS_AERO							VARCHAR(5)					NOT NULL,
	IS_FRAME_ONLY				VARCHAR(5)					NOT NULL,
	IS_GEAR_SHIFT_ELECTRIC		VARCHAR(5)					NOT NULL,
	RIM 								VARCHAR(64) CHARACTER SET UTF8MB4
);

CREATE TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS
(
	RACE_BIKE_ID				BIGINT							NOT NULL,
	ELEMENT							VARCHAR(64) CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_RACE_BIKE_AVAILABLE_GROUP_SETS$RACE_BIKE$ELEMENT UNIQUE (RACE_BIKE_ID,ELEMENT)
);

CREATE TABLE DOM_CLIENT_REGION_IN_PROGRESS
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	REGION							VARCHAR(64) CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_CLIENT_REGION_IN_PROGRESS$REGION UNIQUE (REGION)
);
CREATE INDEX DOM_IDX_CLIENT_REGION_IN_PROGRESS$LAST_MODIFIED ON DOM_CLIENT_REGION_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_CLIENT
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	FIRST_NAME					VARCHAR(256) CHARACTER SET UTF8MB4		NOT NULL,
	GENDER							VARCHAR(64) CHARACTER SET UTF8MB4,
	COUNTRY							VARCHAR(64) CHARACTER SET UTF8MB4,
	BIKE_SIZE						VARCHAR(64) CHARACTER SET UTF8MB4,
	DISPOSABLE_MONEY		DOUBLE							NOT NULL,

	CONSTRAINT UNIQUE_CLIENT$FIRST$COUNTRY UNIQUE (FIRST_NAME,COUNTRY)
);
CREATE INDEX DOM_IDX_CLIENT$LAST_MODIFIED ON DOM_CLIENT (LAST_MODIFIED);
CREATE INDEX DOM_IDX_CLIENT$BIKE_SIZE ON DOM_CLIENT (BIKE_SIZE);

CREATE TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP
(
	CLIENT_ID						BIGINT							NOT NULL,
	ENTRY_KEY						VARCHAR(256) CHARACTER SET UTF8MB4,
	ENTRY_VALUE					DOUBLE,

	CONSTRAINT UNIQUE_CLIENT_WANTED_BIKES_MAX_PRICE_MAP$CLIENT$ENTRY UNIQUE (CLIENT_ID,ENTRY_KEY)
);

CREATE TABLE DOM_ORDER_IN_PROGRESS
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_ORDER_IN_PROGRESS$LAST_MODIFIED ON DOM_ORDER_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_ORDER
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	WAS_CANCELED				VARCHAR(5)					NOT NULL,
	ORDER_DATE					DATETIME,
	INVOICE_DATE				DATETIME,
	PAY_DATE						DATETIME,
	DELIVERY_DATE				DATETIME,
	CLIENT_ID						BIGINT							NOT NULL,
	BIKE_ID							BIGINT							NOT NULL
);
CREATE INDEX DOM_IDX_ORDER$LAST_MODIFIED ON DOM_ORDER (LAST_MODIFIED);

CREATE TABLE DOM_CITY_BIKE
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY
);

CREATE TABLE DOM_CITY_BIKE_FEATURES
(
	CITY_BIKE_ID				BIGINT							NOT NULL,
	ELEMENT							VARCHAR(64) CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_CITY_BIKE_FEATURES$CITY_BIKE$ELEMENT UNIQUE (CITY_BIKE_ID,ELEMENT)
);

CREATE TABLE DOM_MTB
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	BIKE_TYPE						VARCHAR(64) CHARACTER SET UTF8MB4,
	SUSPENSION					VARCHAR(64) CHARACTER SET UTF8MB4,
	WHEEL_SIZE					VARCHAR(64) CHARACTER SET UTF8MB4
);


ALTER TABLE DOM_BIKE ADD CONSTRAINT FK_BIKE$MANUFACTURER FOREIGN KEY (MANUFACTURER_ID) REFERENCES DOM_MANUFACTURER(ID);
ALTER TABLE DOM_BIKE_SIZES ADD CONSTRAINT FK_BIKE_SIZES$BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID);
ALTER TABLE DOM_BIKE_AVAILABILITY_MAP ADD CONSTRAINT FK_BIKE_AVAILABILITY_MAP$BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID);
ALTER TABLE DOM_RACE_BIKE ADD CONSTRAINT FK_RACE_BIKE$INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_RACE_BIKE_AVAILABLE_GROUP_SETS ADD CONSTRAINT FK_RACE_BIKE_AVAILABLE_GROUP_SETS$RACE_BIKE FOREIGN KEY (RACE_BIKE_ID) REFERENCES DOM_RACE_BIKE(ID);
ALTER TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP ADD CONSTRAINT FK_CLIENT_WANTED_BIKES_MAX_PRICE_MAP$CLIENT FOREIGN KEY (CLIENT_ID) REFERENCES DOM_CLIENT(ID);
ALTER TABLE DOM_ORDER ADD CONSTRAINT FK_ORDER$CLIENT FOREIGN KEY (CLIENT_ID) REFERENCES DOM_CLIENT(ID) ON DELETE CASCADE;
ALTER TABLE DOM_ORDER ADD CONSTRAINT FK_ORDER$BIKE FOREIGN KEY (BIKE_ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_CITY_BIKE ADD CONSTRAINT FK_CITY_BIKE$INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
ALTER TABLE DOM_CITY_BIKE_FEATURES ADD CONSTRAINT FK_CITY_BIKE_FEATURES$CITY_BIKE FOREIGN KEY (CITY_BIKE_ID) REFERENCES DOM_CITY_BIKE(ID);
ALTER TABLE DOM_MTB ADD CONSTRAINT FK_MTB$INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_BIKE(ID) ON DELETE CASCADE;
