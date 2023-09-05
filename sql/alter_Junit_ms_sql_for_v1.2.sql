ALTER TABLE DOM_A ALTER COLUMN 	S  								NVARCHAR(16);
ALTER TABLE DOM_A DROP CONSTRAINT UNIQUE_A#S;
ALTER TABLE DOM_A ALTER COLUMN 	DOM_TYPE						NVARCHAR(64);
ALTER TABLE DOM_A_STRINGS ADD 	ELEMENT_ORDER				INTEGER;
ALTER TABLE DOM_A_STRINGS DROP CONSTRAINT UNIQUE_A_STRINGS#A#ELEMENT;
ALTER TABLE DOM_A_DOUBLE_SET DROP COLUMN ELEMENT_ORDER;
ALTER TABLE DOM_A_DOUBLE_SET ADD CONSTRAINT UNIQUE_A_DOUBLE_SET#A#ELEMENT UNIQUE (A_ID,ELEMENT);

ALTER TABLE DOM_A ADD CONSTRAINT UNIQUE_A#I#INTEGER UNIQUE (I,INTEGER_VALUE);
DROP INDEX DOM_IDX_A#LONG_VALUE;
CREATE INDEX DOM_IDX_A#L ON DOM_A (L);
CREATE INDEX DOM_IDX_A#LONG_VALUE ON DOM_A (LONG_VALUE);

DROP TABLE DOM_REMOVED_CLASS;
