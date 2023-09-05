CREATE TABLE DOM_MANUFACTURER
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	COUNTRY							VARCHAR(64) CHARACTER SET UTF8MB4
);
CREATE INDEX DOM_IDX_MANUFACTURER$LAST_MODIFIED ON DOM_MANUFACTURER (LAST_MODIFIED);


ALTER TABLE DOM_BIKE MODIFY COLUMN 	MODEL								VARCHAR(64) CHARACTER SET UTF8MB4;
ALTER TABLE DOM_BIKE ADD 	DROPPED							BIGINT;
ALTER TABLE DOM_BIKE_SIZES DROP COLUMN ELEMENT_ORDER;
ALTER TABLE DOM_BIKE_SIZES ADD CONSTRAINT UNIQUE_BIKE_SIZES$BIKE$ELEMENT UNIQUE (BIKE_ID,ELEMENT);
ALTER TABLE DOM_BIKE MODIFY COLUMN 	PRICE								DOUBLE;
ALTER TABLE DOM_BIKE ADD 	MANUFACTURER_ID			BIGINT;
ALTER TABLE DOM_BIKE ADD CONSTRAINT FK_BIKE$MANUFACTURER FOREIGN KEY (MANUFACTURER_ID) REFERENCES DOM_MANUFACTURER(ID);

ALTER TABLE DOM_BIKE DROP INDEX UNIQUE_BIKE$MODEL;
ALTER TABLE DOM_BIKE ADD CONSTRAINT UNIQUE_BIKE$MANUFACTURER$MODEL UNIQUE (MANUFACTURER_ID,MODEL);
CREATE INDEX DOM_IDX_BIKE$WEIGHT ON DOM_BIKE (WEIGHT);

CREATE TABLE DOM_CITY_BIKE_FEATURES
(
	CITY_BIKE_ID				BIGINT							NOT NULL,
	ELEMENT							VARCHAR(64) CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_CITY_BIKE_FEATURES$CITY_BIKE$ELEMENT UNIQUE (CITY_BIKE_ID,ELEMENT)
);

CREATE TABLE DOM_CLIENT
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	FIRST_NAME					VARCHAR(256) CHARACTER SET UTF8MB4		NOT NULL,
	GENDER							VARCHAR(64) CHARACTER SET UTF8MB4,
	COUNTRY							VARCHAR(64) CHARACTER SET UTF8MB4,
	BIKE_SIZE						VARCHAR(64) CHARACTER SET UTF8MB4,

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


ALTER TABLE DOM_CITY_BIKE_FEATURES ADD CONSTRAINT FK_CITY_BIKE_FEATURES$CITY_BIKE FOREIGN KEY (CITY_BIKE_ID) REFERENCES DOM_CITY_BIKE(ID);
ALTER TABLE DOM_CLIENT_WANTED_BIKES_MAX_PRICE_MAP ADD CONSTRAINT FK_CLIENT_WANTED_BIKES_MAX_PRICE_MAP$CLIENT FOREIGN KEY (CLIENT_ID) REFERENCES DOM_CLIENT(ID);