ALTER TABLE DOM_A ADD 	L  								BIGINT							NOT NULL;
ALTER TABLE DOM_A ALTER COLUMN 	LONG_VALUE					BIGINT;
ALTER TABLE DOM_A ADD 	S  								NVARCHAR(MAX);
ALTER TABLE DOM_A ADD CONSTRAINT UNIQUE_A#S UNIQUE (S);
ALTER TABLE DOM_A ALTER COLUMN 	DOM_TYPE						NVARCHAR(64)				NOT NULL;
ALTER TABLE DOM_A DROP COLUMN REMOVED_FIELD;
ALTER TABLE DOM_A DROP CONSTRAINT FK_A#REMOVED_REF_FIELD;
ALTER TABLE DOM_A DROP COLUMN REMOVED_REF_FIELD_ID;
DROP TABLE DOM_A_REMOVED_COLLECTION_FIELD;
CREATE TABLE DOM_A_STRINGS
(
	A_ID								BIGINT							NOT NULL,
	ELEMENT							NVARCHAR(256),

	CONSTRAINT UNIQUE_A_STRINGS#A#ELEMENT UNIQUE (A_ID,ELEMENT)
);

ALTER TABLE DOM_A_DOUBLE_SET ADD 	ELEMENT_ORDER				INTEGER;
ALTER TABLE DOM_A_DOUBLE_SET DROP CONSTRAINT UNIQUE_A_DOUBLE_SET#A#ELEMENT;
ALTER TABLE DOM_A ADD 	O_ID								BIGINT;
ALTER TABLE DOM_A ADD CONSTRAINT FK_A#O FOREIGN KEY (O_ID) REFERENCES DOM_O(ID) ON DELETE CASCADE;

ALTER TABLE DOM_A DROP CONSTRAINT UNIQUE_A#I;
ALTER TABLE DOM_A ADD CONSTRAINT UNIQUE_A#I#INTEGER UNIQUE (I,INTEGER_VALUE);
CREATE INDEX DOM_IDX_A#L ON DOM_A (L);

CREATE TABLE DOM_Z
(
	DOMAIN_CLASS				NVARCHAR(64),
	ID 								BIGINT							PRIMARY KEY,
	LAST_MODIFIED				DATETIME,
	X_ID								BIGINT
);
CREATE INDEX DOM_IDX_Z#LAST_MODIFIED ON DOM_Z (LAST_MODIFIED);


ALTER TABLE DOM_A_STRINGS ADD CONSTRAINT FK_A_STRINGS#A FOREIGN KEY (A_ID) REFERENCES DOM_A(ID);
ALTER TABLE DOM_Z ADD CONSTRAINT FK_Z#X FOREIGN KEY (X_ID) REFERENCES DOM_X(ID) ON DELETE CASCADE;