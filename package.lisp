(defpackage #:pjs-sqlite
  (:use #:cl #:pjs-utils)
  (:export #:with-recursive-transaction
           #:create-all-tables
	   #:create-index
           #:bool-col
           #:sql-name
	   #:sql-and-equal-string
	   #:with-sqlite-statements
           #:do-sqlite-query
	   #:do-sqlite-query*
           #:sqlite-execute-to-flat-list
           #:defsqlite-table
           #:key-in-table
           #:select-or-insert
	   #:with-sqlite-queries
	   #:create-with-db-macro))

;; yes this is ugly

(in-package :sqlite)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (dolist (sym '(resultset-columns-names
		 resultset-columns-count))
    (export sym :sqlite)))
