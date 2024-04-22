ALTER TABLE DOM_AB DROP FOREIGN KEY FK_AB$INHERITANCE;
ALTER TABLE DOM_SEC_B DROP FOREIGN KEY FK_SEC_B$AA;
ALTER TABLE DOM_Y DROP FOREIGN KEY FK_Y$Z;
ALTER TABLE DOM_Y DROP FOREIGN KEY FK_Y$Y;
ALTER TABLE DOM_Z DROP FOREIGN KEY FK_Z$X;
ALTER TABLE DOM_X_IS DROP FOREIGN KEY FK_X_IS$X;
ALTER TABLE DOM_X DROP FOREIGN KEY FK_X$Y;
ALTER TABLE DOM_X DROP FOREIGN KEY FK_X$A;
ALTER TABLE DOM_AA DROP FOREIGN KEY FK_AA$INHERITANCE;
ALTER TABLE DOM_A_MAP_OF_MAPS DROP FOREIGN KEY FK_A_MAP_OF_MAPS$A;
ALTER TABLE DOM_A_MAP_OF_LISTS DROP FOREIGN KEY FK_A_MAP_OF_LISTS$A;
ALTER TABLE DOM_A_LIST_OF_MAPS DROP FOREIGN KEY FK_A_LIST_OF_MAPS$A;
ALTER TABLE DOM_A_LIST_OF_LISTS DROP FOREIGN KEY FK_A_LIST_OF_LISTS$A;
ALTER TABLE DOM_A_BIG_DECIMAL_MAP DROP FOREIGN KEY FK_A_BIG_DECIMAL_MAP$A;
ALTER TABLE DOM_A_DOUBLE_SET DROP FOREIGN KEY FK_A_DOUBLE_SET$A;
ALTER TABLE DOM_A_STRINGS DROP FOREIGN KEY FK_A_STRINGS$A;
ALTER TABLE DOM_A_STRING_ARRAY DROP FOREIGN KEY FK_A_STRING_ARRAY$A;
ALTER TABLE DOM_A DROP FOREIGN KEY FK_A$O;
ALTER TABLE DOM_A_INNER DROP FOREIGN KEY FK_A_INNER$A;
ALTER TABLE DOM_C DROP FOREIGN KEY FK_C$C;

DROP TABLE DOM_AB;
DROP TABLE DOM_SEC_B;
DROP TABLE DOM_Y;
DROP TABLE DOM_Z;
DROP TABLE DOM_X_IS;
DROP TABLE DOM_X;
DROP TABLE DOM_X_IN_PROGRESS;
DROP TABLE DOM_AA;
DROP TABLE DOM_A_MAP_OF_MAPS;
DROP TABLE DOM_A_MAP_OF_LISTS;
DROP TABLE DOM_A_LIST_OF_MAPS;
DROP TABLE DOM_A_LIST_OF_LISTS;
DROP TABLE DOM_A_BIG_DECIMAL_MAP;
DROP TABLE DOM_A_DOUBLE_SET;
DROP TABLE DOM_A_STRINGS;
DROP TABLE DOM_A_STRING_ARRAY;
DROP TABLE DOM_A;
DROP TABLE DOM_A_INNER;
DROP TABLE DOM_O;
DROP TABLE DOM_C;

CREATE TABLE DOM_C
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	NAME								VARCHAR(512) CHARACTER SET UTF8MB4,
	C_ID								BIGINT
);
CREATE INDEX DOM_IDX_C$LAST_MODIFIED ON DOM_C (LAST_MODIFIED);

CREATE TABLE DOM_O
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_O$LAST_MODIFIED ON DOM_O (LAST_MODIFIED);

CREATE TABLE DOM_A_INNER
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	A_ID								BIGINT
);
CREATE INDEX DOM_IDX_A_INNER$LAST_MODIFIED ON DOM_A_INNER (LAST_MODIFIED);

CREATE TABLE DOM_A
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	BOOLEAN							VARCHAR(5)					NOT NULL,
	BOOLEAN_VALUE				VARCHAR(5),
	SH 								SMALLINT						NOT NULL,
	SHORT_VALUE					SMALLINT,
	I  								INTEGER							NOT NULL,
	INTEGER_VALUE				INTEGER,
	L  								BIGINT							NOT NULL,
	LONG_VALUE					BIGINT,
	D  								DOUBLE							NOT NULL,
	DOUBLE_VALUE				DOUBLE,
	C  								VARCHAR(1)					NOT NULL,
	CHAR_VALUE					VARCHAR(1),
	BIG_INTEGER_VALUE		BIGINT,
	BIG_DECIMAL_VALUE		DOUBLE,
	DATETIME						DATETIME,
	DOM_DATE						DATE,
	TIME								TIME,
	S  								VARCHAR(16) CHARACTER SET UTF8MB4,
	STRUCTURE						VARCHAR(512) CHARACTER SET UTF8MB4,
	PICTURE							LONGBLOB,
	DOM_LONGTEXT				LONGTEXT,
	DOM_FILE						LONGBLOB,
	DOM_TYPE						VARCHAR(64) CHARACTER SET UTF8MB4		NOT NULL,
	SEC_SECRET_STRING		VARCHAR(512) CHARACTER SET UTF8MB4,
	PWD 								VARCHAR(512) CHARACTER SET UTF8MB4,
	DEPRECATED_FIELD		VARCHAR(512) CHARACTER SET UTF8MB4,
	O_ID								BIGINT,

	CONSTRAINT UNIQUE_A$S UNIQUE (S),

	CONSTRAINT UNIQUE_A$I$INTEGER UNIQUE (I,INTEGER_VALUE)
);
CREATE INDEX DOM_IDX_A$LAST_MODIFIED ON DOM_A (LAST_MODIFIED);
CREATE INDEX DOM_IDX_A$L ON DOM_A (L);
CREATE INDEX DOM_IDX_A$LONG_VALUE ON DOM_A (LONG_VALUE);

CREATE TABLE DOM_A_STRING_ARRAY
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							VARCHAR(512) CHARACTER SET UTF8MB4,
	ELEMENT_ORDER				BIGINT
);

CREATE TABLE DOM_A_STRINGS
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							VARCHAR(512) CHARACTER SET UTF8MB4,
	ELEMENT_ORDER				BIGINT,

	CONSTRAINT UNIQUE_A_STRINGS$A$ELEMENT UNIQUE (A_ID,ELEMENT_ORDER)
);

CREATE TABLE DOM_A_DOUBLE_SET
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							DOUBLE,

	CONSTRAINT UNIQUE_A_DOUBLE_SET$A$ELEMENT UNIQUE (A_ID,ELEMENT)
);

CREATE TABLE DOM_A_BIG_DECIMAL_MAP
(
	A_ID								BIGINT							NOT NULL,
	ENTRY_KEY						VARCHAR(512) CHARACTER SET UTF8MB4,
	ENTRY_VALUE					DOUBLE,

	CONSTRAINT UNIQUE_A_BIG_DECIMAL_MAP$A$ENTRY UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_A_LIST_OF_LISTS
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							LONGTEXT CHARACTER SET UTF8MB4,
	ELEMENT_ORDER				BIGINT,

	CONSTRAINT UNIQUE_A_LIST_OF_LISTS$A$ELEMENT UNIQUE (A_ID,ELEMENT_ORDER)
);

CREATE TABLE DOM_A_LIST_OF_MAPS
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							LONGTEXT CHARACTER SET UTF8MB4,
	ELEMENT_ORDER				BIGINT,

	CONSTRAINT UNIQUE_A_LIST_OF_MAPS$A$ELEMENT UNIQUE (A_ID,ELEMENT_ORDER)
);

CREATE TABLE DOM_A_MAP_OF_LISTS
(
	A_ID								BIGINT							NOT NULL,
	ENTRY_KEY						BIGINT,
	ENTRY_VALUE					LONGTEXT CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_A_MAP_OF_LISTS$A$ENTRY UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_A_MAP_OF_MAPS
(
	A_ID								BIGINT							NOT NULL,
	ENTRY_KEY						VARCHAR(512) CHARACTER SET UTF8MB4,
	ENTRY_VALUE					LONGTEXT CHARACTER SET UTF8MB4,

	CONSTRAINT UNIQUE_A_MAP_OF_MAPS$A$ENTRY UNIQUE (A_ID,ENTRY_KEY)
);

CREATE TABLE DOM_AA
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY
);

CREATE TABLE DOM_X_IN_PROGRESS
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME
);
CREATE INDEX DOM_IDX_X_IN_PROGRESS$LAST_MODIFIED ON DOM_X_IN_PROGRESS (LAST_MODIFIED);

CREATE TABLE DOM_X
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	S  								VARCHAR(512) CHARACTER SET UTF8MB4,
	A_ID								BIGINT,
	Y_ID								BIGINT
);
CREATE INDEX DOM_IDX_X$LAST_MODIFIED ON DOM_X (LAST_MODIFIED);

CREATE TABLE DOM_X_IS
(
	X_ID								BIGINT							NOT NULL,
	ELEMENT							INTEGER,
	ELEMENT_ORDER				BIGINT,

	CONSTRAINT UNIQUE_X_IS$X$ELEMENT UNIQUE (X_ID,ELEMENT_ORDER)
);

CREATE TABLE DOM_Z
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	X_ID								BIGINT
);
CREATE INDEX DOM_IDX_Z$LAST_MODIFIED ON DOM_Z (LAST_MODIFIED);

CREATE TABLE DOM_Y
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	Y_ID								BIGINT,
	Z_ID								BIGINT							NOT NULL
);
CREATE INDEX DOM_IDX_Y$LAST_MODIFIED ON DOM_Y (LAST_MODIFIED);

CREATE TABLE DOM_SEC_B
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	NAME								VARCHAR(512) CHARACTER SET UTF8MB4,
	AA_ID								BIGINT
);
CREATE INDEX DOM_IDX_SEC_B$LAST_MODIFIED ON DOM_SEC_B (LAST_MODIFIED);

CREATE TABLE DOM_AB
(
	DOMAIN_CLASS				VARCHAR(64) CHARACTER SET UTF8MB4,
	ID 								BIGINT							PRIMARY KEY
);


ALTER TABLE DOM_C ADD CONSTRAINT FK_C$C FOREIGN KEY (C_ID) REFERENCES DOM_C(ID);
ALTER TABLE DOM_A_INNER ADD CONSTRAINT FK_A_INNER$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A ADD CONSTRAINT FK_A$O FOREIGN KEY (O_ID) REFERENCES DOM_O(ID) ON DELETE CASCADE;
ALTER TABLE DOM_A_STRING_ARRAY ADD CONSTRAINT FK_A_STRING_ARRAY$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_STRINGS ADD CONSTRAINT FK_A_STRINGS$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_DOUBLE_SET ADD CONSTRAINT FK_A_DOUBLE_SET$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_BIG_DECIMAL_MAP ADD CONSTRAINT FK_A_BIG_DECIMAL_MAP$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_LIST_OF_LISTS ADD CONSTRAINT FK_A_LIST_OF_LISTS$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_LIST_OF_MAPS ADD CONSTRAINT FK_A_LIST_OF_MAPS$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_MAP_OF_LISTS ADD CONSTRAINT FK_A_MAP_OF_LISTS$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_A_MAP_OF_MAPS ADD CONSTRAINT FK_A_MAP_OF_MAPS$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_AA ADD CONSTRAINT FK_AA$INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X ADD CONSTRAINT FK_X$A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X ADD CONSTRAINT FK_X$Y FOREIGN KEY (Y_ID) REFERENCES DOM_Y(ID) ON DELETE CASCADE;
ALTER TABLE DOM_X_IS ADD CONSTRAINT FK_X_IS$X FOREIGN KEY (X_ID) REFERENCES DOM_X(ID);
ALTER TABLE DOM_Z ADD CONSTRAINT FK_Z$X FOREIGN KEY (X_ID) REFERENCES DOM_X(ID) ON DELETE CASCADE;
ALTER TABLE DOM_Y ADD CONSTRAINT FK_Y$Y FOREIGN KEY (Y_ID) REFERENCES DOM_Y(ID) ON DELETE CASCADE;
ALTER TABLE DOM_Y ADD CONSTRAINT FK_Y$Z FOREIGN KEY (Z_ID) REFERENCES DOM_Z(ID) ON DELETE CASCADE;
ALTER TABLE DOM_SEC_B ADD CONSTRAINT FK_SEC_B$AA FOREIGN KEY (AA_ID) REFERENCES DOM_AA(ID);
ALTER TABLE DOM_AB ADD CONSTRAINT FK_AB$INHERITANCE FOREIGN KEY (ID) REFERENCES DOM_A(ID) ON DELETE CASCADE;
