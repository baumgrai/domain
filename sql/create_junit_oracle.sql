ALTER TABLE DOM_X_IS DROP CONSTRAINT FK_X_IS#X;
ALTER TABLE DOM_X DROP CONSTRAINT FK_X#Y;
ALTER TABLE DOM_X DROP CONSTRAINT FK_X#A;
ALTER TABLE DOM_Y DROP CONSTRAINT FK_Y#Z;
ALTER TABLE DOM_Y DROP CONSTRAINT FK_Y#Y;
ALTER TABLE DOM_Z DROP CONSTRAINT FK_Z#X;
ALTER TABLE DOM_AB DROP CONSTRAINT FK_AB#INHERITANCE;
ALTER TABLE DOM_SEC_B DROP CONSTRAINT FK_SEC_B#AA;
ALTER TABLE DOM_AA DROP CONSTRAINT FK_AA#INHERITANCE;
ALTER TABLE DOM_A_MAP_OF_MAPS DROP CONSTRAINT FK_A_MAP_OF_MAPS#A;
ALTER TABLE DOM_A_MAP_OF_LISTS DROP CONSTRAINT FK_A_MAP_OF_LISTS#A;
ALTER TABLE DOM_A_LIST_OF_MAPS DROP CONSTRAINT FK_A_LIST_OF_MAPS#A;
ALTER TABLE DOM_A_LIST_OF_LISTS DROP CONSTRAINT FK_A_LIST_OF_LISTS#A;
ALTER TABLE DOM_A_BIG_DECIMAL_MAP DROP CONSTRAINT FK_A_BIG_DECIMAL_MAP#A;
ALTER TABLE DOM_A_DOUBLE_SET DROP CONSTRAINT FK_A_DOUBLE_SET#A;
ALTER TABLE DOM_A_STRINGS DROP CONSTRAINT FK_A_STRINGS#A;
ALTER TABLE DOM_A DROP CONSTRAINT FK_A#O;
ALTER TABLE DOM_A_INNER DROP CONSTRAINT FK_A_INNER#A;
ALTER TABLE DOM_C DROP CONSTRAINT FK_C#C;

DROP TABLE DOM_X_IS;
DROP TABLE DOM_X;
DROP TABLE DOM_X_IN_PROGRESS;
DROP TABLE DOM_Y;
DROP TABLE DOM_Z;
DROP TABLE DOM_AB;
DROP TABLE DOM_SEC_B;
DROP TABLE DOM_AA;
DROP TABLE DOM_A_MAP_OF_MAPS;
DROP TABLE DOM_A_MAP_OF_LISTS;
DROP TABLE DOM_A_LIST_OF_MAPS;
DROP TABLE DOM_A_LIST_OF_LISTS;
DROP TABLE DOM_A_BIG_DECIMAL_MAP;
DROP TABLE DOM_A_DOUBLE_SET;
DROP TABLE DOM_A_STRINGS;
DROP TABLE DOM_A;
DROP TABLE DOM_A_INNER;
DROP TABLE DOM_O;
DROP TABLE DOM_C;

CREATE TABLE DOM_C
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	NAME								NVARCHAR2(1024),
	C_ID								NUMBER
);
CREATE INDEX DOM_IDX_C#LAST_MODIFIED ON DOM_C (LAST_MODIFIED);

CREATE TABLE DOM_O
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP
);
CREATE INDEX DOM_IDX_O#LAST_MODIFIED ON DOM_O (LAST_MODIFIED);

CREATE TABLE DOM_A_INNER
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	A_ID								NUMBER
);
CREATE INDEX DOM_IDX_A_INNER#LAST_MODIFIED ON DOM_A_INNER (LAST_MODIFIED);

CREATE TABLE DOM_A
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	BOOLEAN							NVARCHAR2(5)				NOT NULL,
	BOOLEAN_VALUE				NVARCHAR2(5),
	I  								NUMBER							NOT NULL,
	INTEGER_VALUE				NUMBER,
	L  								NUMBER							NOT NULL,
	LONG_VALUE					NUMBER,
	D  								NUMBER							NOT NULL,
	DOUBLE_VALUE				NUMBER,
	BIG_INTEGER_VALUE		NUMBER,
	BIG_DECIMAL_VALUE		NUMBER,
	DATETIME						TIMESTAMP,
	S  								NVARCHAR2(16),
	BYTES								BLOB,
	PICTURE							BLOB,
	DOM_FILE						NVARCHAR2(1024),
	DOM_TYPE						NVARCHAR2(64)				NOT NULL,
	SEC_SECRET_STRING		NVARCHAR2(1024),
	PWD 								NVARCHAR2(1024),
	DEPRECATED_FIELD		NVARCHAR2(1024),
	O_ID								NUMBER,

	CONSTRAINT UNIQUE_A#S UNIQUE (S),

	CONSTRAINT UNIQUE_A#I#INTEGER UNIQUE (I,INTEGER_VALUE)
);
CREATE INDEX DOM_IDX_A#LAST_MODIFIED ON DOM_A (LAST_MODIFIED);
CREATE INDEX DOM_IDX_A#L ON DOM_A (L);
CREATE INDEX DOM_IDX_A#LONG_VALUE ON DOM_A (LONG_VALUE);

CREATE TABLE DOM_A_STRINGS
(
	A_ID								NUMBER							NOT NULL,
	ELEMENT							NVARCHAR2(1024),
	ELEMENT_ORDER				NUMBER
);

CREATE TABLE DOM_A_DOUBLE_SET
(
	A_ID								NUMBER							NOT NULL,
	ELEMENT							NUMBER,

	CONSTRAINT UNIQUE_A_DOUBLE_SET#A#ELEMENT UNIQUE (A_ID,ELEMENT)
);

CREATE TABLE DOM_A_BIG_DECIMAL_MAP
(
	A_ID								NUMBER							NOT NULL,
	ENTRY_KEY						NVARCHAR2(512),
	ENTRY_VALUE					NUMBER,

	CONSTRAINT UNIQUE_A_BIG_DECIMAL_MAP#A#ENT UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_A_LIST_OF_LISTS
(
	A_ID								NUMBER							NOT NULL,
	ELEMENT							NVARCHAR2(1024),
	ELEMENT_ORDER				NUMBER
);

CREATE TABLE DOM_A_LIST_OF_MAPS
(
	A_ID								NUMBER							NOT NULL,
	ELEMENT							NVARCHAR2(1024),
	ELEMENT_ORDER				NUMBER
);

CREATE TABLE DOM_A_MAP_OF_LISTS
(
	A_ID								NUMBER							NOT NULL,
	ENTRY_KEY						NUMBER,
	ENTRY_VALUE					NVARCHAR2(1024),

	CONSTRAINT UNIQUE_A_MAP_OF_LISTS#A#ENTRY UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_A_MAP_OF_MAPS
(
	A_ID								NUMBER							NOT NULL,
	ENTRY_KEY						NVARCHAR2(512),
	ENTRY_VALUE					NVARCHAR2(1024),

	CONSTRAINT UNIQUE_A_MAP_OF_MAPS#A#ENTRY UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_AA
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY
);

CREATE TABLE DOM_SEC_B
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	NAME								NVARCHAR2(1024),
	AA_ID								NUMBER
);
CREATE INDEX DOM_IDX_SEC_B#LAST_MODIFIED ON DOM_SEC_B (LAST_MODIFIED);

CREATE TABLE DOM_AB
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY
);

CREATE TABLE DOM_Z
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	X_ID								NUMBER
);
CREATE INDEX DOM_IDX_Z#LAST_MODIFIED ON DOM_Z (LAST_MODIFIED);

CREATE TABLE DOM_Y
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	Y_ID								NUMBER,
	Z_ID								NUMBER							NOT NULL
);
CREATE INDEX DOM_IDX_Y#LAST_MODIFIED ON DOM_Y (LAST_MODIFIED);

CREATE TABLE DOM_X_IN_PROGRESS
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP
);
CREATE INDEX DOM_IDX_X_IN_PROGRESS#LAST_MOD ON DOM_X_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_X
(
	DOMAIN_CLASS				NVARCHAR2(64),
	ID 								NUMBER							PRIMARY KEY,
	LAST_MODIFIED				TIMESTAMP,
	S  								NVARCHAR2(1024),
	A_ID								NUMBER,
	Y_ID								NUMBER
);
CREATE INDEX DOM_IDX_X#LAST_MODIFIED ON DOM_X (LAST_MODIFIED);

CREATE TABLE DOM_X_IS
(
	X_ID								NUMBER							NOT NULL,
	ELEMENT							NUMBER,
	ELEMENT_ORDER				NUMBER
);


ALTER TABLE DOM_C ADD CONSTRAINT FK_C#C FOREIGN KEY (C_ID) REFERENCES DOM_C(ID);
ALTER TABLE DOM_A_INNER ADD CONSTRAINT FK_A_INNER#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A ADD CONSTRAINT FK_A#O FOREIGN KEY (O_ID) REFERENCES DOM_O(ID) ON DELETE CASCADE;
ALTER TABLE DOM_A_STRINGS ADD CONSTRAINT FK_A_STRINGS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_DOUBLE_SET ADD CONSTRAINT FK_A_DOUBLE_SET#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_BIG_DECIMAL_MAP ADD CONSTRAINT FK_A_BIG_DECIMAL_MAP#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_LIST_OF_LISTS ADD CONSTRAINT FK_A_LIST_OF_LISTS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_LIST_OF_MAPS ADD CONSTRAINT FK_A_LIST_OF_MAPS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_MAP_OF_LISTS ADD CONSTRAINT FK_A_MAP_OF_LISTS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_MAP_OF_MAPS ADD CONSTRAINT FK_A_MAP_OF_MAPS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_AA ADD CONSTRAINT FK_AA#INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
ALTER TABLE DOM_SEC_B ADD CONSTRAINT FK_SEC_B#AA FOREIGN KEY (AA_ID) REFERENCES DOM_AA(ID);
ALTER TABLE DOM_AB ADD CONSTRAINT FK_AB#INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
ALTER TABLE DOM_Z ADD CONSTRAINT FK_Z#X FOREIGN KEY (X_ID) REFERENCES DOM_X(ID) ON DELETE CASCADE;
ALTER TABLE DOM_Y ADD CONSTRAINT FK_Y#Y FOREIGN KEY (Y_ID) REFERENCES DOM_Y(ID) ON DELETE CASCADE;
ALTER TABLE DOM_Y ADD CONSTRAINT FK_Y#Z FOREIGN KEY (Z_ID) REFERENCES DOM_Z(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X ADD CONSTRAINT FK_X#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X ADD CONSTRAINT FK_X#Y FOREIGN KEY (Y_ID) REFERENCES DOM_Y(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X_IS ADD CONSTRAINT FK_X_IS#X FOREIGN KEY (X_ID) REFERENCES DOM_X(ID);
