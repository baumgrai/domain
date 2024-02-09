package com.icx.dom.junit.tests;

import com.icx.jdbc.SqlDb.DbType;

public class Helpers {

	static DbType dbType = null;

	static String getLocal(DbType dbType) {

		if (dbType == DbType.MYSQL) {
			return "local/mysql/junit";
		}
		else if (dbType == DbType.MS_SQL) {
			return "local/ms_sql/junit";
		}
		else if (dbType == DbType.ORACLE) {
			return "local/oracle/junit";
		}
		else {
			return null;
		}
	}

}
