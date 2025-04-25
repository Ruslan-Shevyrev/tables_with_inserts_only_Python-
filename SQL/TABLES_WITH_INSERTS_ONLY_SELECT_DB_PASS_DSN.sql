SELECT DBSYSPWD, dbadmindata.pkg_tns.getcstrbyalias(tnsalias) AS DSN
	FROM DBADMINDATA.ORADBINFO
	WHERE DBID = :dbid