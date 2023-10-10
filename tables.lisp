(in-package #:pjs-sqlite)

(defparameter *sqlite-tables*
  (make-hash-table))

(defmacro column-is-tests (&rest content)
  `(progn
     ,@ (loop #:while content
	      #:collect (let ((name (intern (string-upcase (sconc "column-is-"
								  (pop content)
								  "-p"))))
			      (test (pop content)))
			  `(defun ,name (col)
			     ,test)))))

(defmacro column-type-tests (&rest content)
  (let ((types (dolist-c (pair (group 2 content))
		 (collect (second pair)))))
    `(column-is-tests
     "legal" (member (second col) ',types)
      ,@ (loop #:with types
	       #:while content
	       #:nconcing (let* ((name (pop content))
				 (type (pop content))
				 (test `(eq ,type
					    (second col))))
			    (push type types)
			    (list name test))))))

(defmacro column-containing-tests (&rest content)
  `(column-is-tests
    ,@ (loop #:while content
	     #:nconcing (let ((name (pop content))
			      (sym (pop content)))
			  (list name
				`(member ,sym (cddr col)))))))

(progn
  (column-type-tests "auto-key" :auto-key
		     "bool" :b
		     "integer" :i
		     "real" :r
		     "text" :t
		     "blob" :blob
		     "atom" :atom
		     "last-modified" :modified)
  (column-containing-tests "multi-key" :pk
			   "unique" :u)
  (column-is-tests "nullable" (member :n (cddr col))
		   "key" (or (column-is-auto-key-p col)
			     (column-is-multi-key-p col))
		   "optional" (or (column-default-value col)
				  (column-is-last-modified-p col))))

(defun column-default-value (col)
  (first-after :default
	       (cddr col)))
	   
(defun column-references-table (col)
  (first-after :fk
	       (cddr col)))

(defun column-predicate (column)
  (symb (first column) "-p"))

(defun column-bind (name col index)
  (let* ((col-name (first col))
	 (col-value (if (column-is-bool-p col)
			`(if (and ,col-name
				  (not (equalp ,col-name "N")))
			     "Y"
			     "N")
			col-name)))
    (if (column-is-nullable-p col)
	`(sqlite:bind-parameter stmt ,index ,col-value)
	;; else
	`(if ,col-name
	     (sqlite:bind-parameter stmt ,index ,col-value)
	     ;; else
	     (error ,(sconc "Missing value for not-null column "
			    (sql-name name)
			    "."
			    (sql-name col-name)))))))

(defun sql-quote-string (s)
  "SQL uses pascal style strings which are single-quoted. and enclosed single-quotes are doubled"
  (let ((result (make-array (* (length s)
			       2)
			    :adjustable t
			    :fill-pointer 0)))
    (vector-push-extend #\' result)
    (dovector (ch s)
      (when (char= ch #\')
	(vector-push-extend #\' result))
      (vector-push-extend ch result))
    (vector-push-extend #\' result)
    (coerce result 'string)))

(defun build-create-table-sql (name columns)
  (with-output-to-string (s)
    (format s 
	    "CREATE TABLE ~a (~%  ~a~%)"
	    (sql-name name)
	    (join +separator-string+
		  (apply #'join
			 +separator-string+
			 (mapcar #'(lambda (col)
				     (unless (column-is-legal-p col)
				       (error "illegal column spec: ~a" col))
				     (sconc (sql-name (first col))
					    (cond
					      ((column-is-integer-p col)
					       " INTEGER")
					      ((column-is-real-p col)
					       " REAL")
					      ((column-is-text-p col)
					       " TEXT")
					      ((column-is-bool-p col)
					       " CHARACTER(1) DEFAULT 'N'")
					      ((column-is-auto-key-p col)
					       " INTEGER PRIMARY KEY")
					      ((column-is-atom-p col)
					       " CHARACTER(25)")
					      ((column-is-blob-p col)
					       " BLOB")
					      ((column-is-last-modified-p col)
					       " CHARACTER(25) DEFAULT CURRENT_TIMESTAMP")
					      (t
					       (error "unhandled column kind (~A)" col)))
					    (awhen (column-default-value col)
					      (etypecase it
						((or number symbol)
						 (format nil " DEFAULT ~w" it))
						(character
						 (let* ((str (make-string 1
									  :initial-element it))
							(quoted (sql-quote-string str)))
						   (format nil " DEFAULT ~a" quoted)))
						(string
						 (format nil " DEFAULT ~a" (sql-quote-string it)))))
					    (awhen (column-references-table col)
					      (format nil
						      " REFERENCES ~a ( ~a ) ON DELETE CASCADE"
						      (sql-name (if (symbolp it)
								    it
								    ;; else
								    (first it)))
						      (sql-name (if (symbolp it)
								    "id"
								    ;; else
								    (second it)))))
					    (when (column-is-unique-p col)
					      " UNIQUE")
					    (unless (column-is-nullable-p col)
					      " NOT NULL")))
				 columns))
		  (when (find-if #'column-is-multi-key-p
				 columns)
		    (sconc "PRIMARY KEY ( "
			   (apply #'join
				  ", "
				  (mapcar #'(lambda (col) (sql-name (first col)))
					  (remove-if-not #'column-is-multi-key-p
							 columns)))
			   " )"))))))

(defun create-table-function-name (name)
  (symb "create-" name "-table"))

(defun create-table-sql-name (name)
  (symb "+create-" name "-sql+"))

(defun temporary-table-name (name)
  (sconc (sql-name name) "_temp"))

(defmacro table-schema-from-db (db name)
  `(sqlite:execute-single ,db
			  ,(sconc "SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = '"
				  (sql-name name)
				  "'")))

(defun defsqlite-table-creator (name columns)
  (let ((create-sql-name (create-table-sql-name name))
	(sql-name (sql-name name))
	(temporary-table-name (temporary-table-name name)))
    `(defun ,(create-table-function-name name) (db log)
       (flet ((log-message (datum fmt &rest args)
		(apply #'format t fmt args)
		(apply log datum fmt args)))
	 (let ((table-schema (table-schema-from-db db ,name)))
	   (labels ((create-table ()
		      (sqlite:execute-non-query db
						,create-sql-name)
		      ,@(mapcar (lambda (col)
				  `(create-index db ,name ,(first col)))
				(remove-if-not #'column-references-table
					       columns))))
	     (cond
	       ((not table-schema)
		(create-table))
	       ((string= table-schema
			 ,create-sql-name)
		;; nothing to do
		nil)
	       (t
		(log-message 'message
			     ,(sconc "TABLE SQL FOR '"
				     sql-name
				     "' DIFFERS, ATTEMPTING MIGRATION~%"))
		(log-message 'message
			     "Old schema was: ~w~%New schema is: ~w~%"
			     table-schema
			     ,create-sql-name)
		(let (success
		      made-table)
		  (with-foreign-keys-disabled db
		    (sqlite:execute-non-query db
					      ,(sconc "ALTER TABLE "
						      sql-name
						      " RENAME TO "
						      temporary-table-name))
		    (unwind-protect
			 (progn
			   (create-table)
			   (setf made-table t)
			   (if (migrate-table-data db
						   ,temporary-table-name
						   ,sql-name)
			       (progn
				 (sqlite:execute-non-query db ,(sconc "DROP TABLE "
								      (temporary-table-name name)))
				 (log-message 'message ,(sconc "AUTOMATED MIGRATION SUCCEEDED, APPARENTLY~%"))
				 (setf success t))
			       ;; else
			       (progn
				 (log-message 'error "AUTOMATED MIGRATION FAILED, HERE BE DRAGONS!~%")
				 (error ,(sconc "can't migrate table " sql-name)))))
		      (unless success
			(when made-table
			  (sqlite:execute-non-query db
						    ,(sconc "DROP TABLE "
							    sql-name)))
			(sqlite:execute-non-query db
						  ,(sconc "ALTER TABLE "
							  temporary-table-name
							  " RENAME TO "
							  sql-name))))))))))))))

(defun defsqlite-table-inserter (name columns)
  (multiple-value-bind (optional-columns required-columns)
      (partition (lambda (col)
		   (or (column-is-optional-p col)
		       (column-is-auto-key-p col))) columns)
    `(defun ,(symb "insert-" name) (db
				   ,@(mapcar #'first required-columns)
				   &key ,@(mapcar #'(lambda (col)
						      (list (first col)
							    nil
							    (column-predicate col)))
						  optional-columns))
       (with-sqlite-statements (db
				 (stmt (sconc "INSERT INTO "
					      ,(sql-name name)
					      " ( "
					      (join ,+separator-string+
						    ,(apply #'join
							    +separator-string+
							    (mapcar (lambda (x)
								      (sql-name (first x)))
								    required-columns))
						    ,@(mapcar (lambda (col)
								`(when ,(column-predicate col)
								   ,(sql-name (first col))))
							      optional-columns))
					      " ) VALUES ( "
					      (join ","
						    ,(apply #'join
							    ","
							    (n-copies (length required-columns)
								      "?"))
						    ,@(mapcar #'(lambda (col)
								  `(when ,(column-predicate col)
								     "?"))
							      optional-columns))
					      " )")))
	 ;; loop for each column, bind-parameter starts at 1 but we incf
	 ,@ (loop #:for i from 1
		  #:for col in required-columns
		  #:collect (column-bind name col i))
	 ,(when optional-columns
	    `(let ((index ,(length required-columns)))
	       ,@(mapcar #'(lambda (col)
			     `(when ,(column-predicate col)
				,(column-bind name col '(incf index))))
			 optional-columns)))
	 ;; stmt from preamble
	 (sqlite:step-statement stmt)
	 (sqlite:reset-statement stmt)
	 (sqlite:last-insert-rowid db)))))

(defun defsqlite-table-updater (name columns)
  (multiple-value-bind (key-columns non-key-columns)
      (partition #'column-is-key-p columns)
    (if (and key-columns non-key-columns)
	(multiple-value-bind (last-modified-columns normal-non-key-columns)
	    (partition #'column-is-last-modified-p non-key-columns)
	  `(defun ,(symb "update-" name) (db
					 ,@(mapcar #'first key-columns)
					 &key
					   ,@(mapcar (lambda (col)
						       `(,(first col) nil ,(column-predicate col)))
						     non-key-columns))
	     (with-sqlite-statements (db
				       (stmt (sconc "UPDATE "
						    ,(sql-name name)
						    " SET "
						    (join ", "
							  ,@(mapcar (lambda (col)
								      `(when ,(column-predicate col)
									 ,(sconc (sql-name (first col)) " = ?")))
								    normal-non-key-columns)
							  ,@ (mapcar (lambda (col)
								       `(if ,(column-predicate col)
									    ,(sconc (sql-name (first col)) " = ?")
									    ,(sconc (sql-name (first col)) " = CURRENT_TIMESTAMP")))
								     last-modified-columns))
						    " WHERE "
						    (join " AND "
							  ,@(mapcar (lambda (col)
								      (let ((name (sql-name (first col))))
									(if (column-is-nullable-p col)
									    `(if ,(first col)
										 ,(sconc name " = ?")
										 ;; else
										 ,(sconc name " IS NULL"))
									    ;; else
									    (sconc name " = ?"))))
								    key-columns)))))
	       ;; column-bind uses 1 for the first index, but we INCF
	       (let ((index 0))
		 ,@(mapcar (lambda (col)
			     `(when ,(column-predicate col)
				,(column-bind name col '(incf index))))
			   (append normal-non-key-columns
				   last-modified-columns))
		 ,@(mapcar (lambda (col)
			     (column-bind name col '(incf index)))
			   key-columns)
		 (sqlite:step-statement stmt)))))
	;; else
	(format *error-output*
		"~&not generating update-~a because it has no ~a~%"
		name
		(if key-columns
		    "non-keys to update"
		    ;; else
		    "no keys to select a row")))))

(defun defsqlite-table-deleter (name columns)
  (let ((key-columns (mapcar #'(lambda (col)
				 (list (first col)
				       (or (member :n col)
					   (column-is-bool-p col))))
			     (remove-if-not #'column-is-key-p columns))))
    (if key-columns
	`(defun ,(symb "delete-" name) (db ,@(mapcar #'first key-columns))
	   (with-sqlite-statements (db
				     (stmt (sconc "DELETE FROM "
						  ,(sql-name name)
						  " WHERE "
						  (join " AND "
							,@(mapcar (lambda (col)
								    (let* ((col-name (first col))
									   (sql-name (sql-name col-name))
									   (not-null (sconc sql-name " = ?"))
									   (null (sconc sql-name " IS NULL")))
								      (if (second col)
									  `(if ,col-name
									       ,not-null
									       ;; else
									       ,null)
									  ;; else
									  not-null)))
								  key-columns)))))
	     (let ((index 0))
	       ,@(mapcar (lambda (col)
			   `(if ,(first col)
				(sqlite:bind-parameter stmt (incf index) ,(first col))
				;; else
				,(unless (second col)
				   `(error ,(sconc "missing value for not-null column " (sql-name name) "."  (sql-name (first col)))))))
			 key-columns)
	       (sqlite:step-statement stmt))))
	(format *error-output* "~&warning: not generating deleter for ~w because it has no keys~%" name))))


;; ========================================================
;;
;; ========================================================

(defmacro register-table-creator (creator)
  `(pushnew ',creator
	    (gethash (symbol-package ',creator)
		     *sqlite-tables*)))

;; ========================================================
;;
;; ========================================================

(defmacro defsqlite-table (name &body columns)
  (let ((creator (create-table-function-name name))
	(*print-case* :upcase))
    `(progn
       (defparameter ,(create-table-sql-name name)
	 ,(build-create-table-sql name columns))
       ;;
       ,(defsqlite-table-creator name columns)
       ;;
       (register-table-creator ,creator)
       ;;
       ,(defsqlite-table-inserter name columns)
       ,(defsqlite-table-updater name columns)
       ,(defsqlite-table-deleter name columns))))

;; ========================================================
;;
;; ========================================================

(defmacro defsqlite-table-enum (name &body cases)
  (let* ((plural-name (symb name "s"))
	 (plural-name-name (symbol-name* plural-name))
	 (sql-name (sql-name plural-name))
	 (creator (create-table-function-name plural-name))
	 (sql (build-create-table-sql plural-name
				      `((id :i :pk)
					(name :t))))
	 (cases (mapcar (lambda (case)
			  (list* (symb "+"
				       name
				       "-"
				       (second case)
				       "+")
				 case))
			cases)))
    `(progn
       ,@ (mapcar (lambda (case)
		    `(defconstant ,(first case)
		       ,(second case)))
		  cases)
	  (defun ,creator (db log-message)
	    (flet ((log-message (datum fmt &rest args)
		     (apply log-message
			    datum
			    fmt
			    args)))
	      (let ((sql ,sql)
		    (db-schema (table-schema-from-db db ,plural-name)))
		(if (not db-schema)
		    (progn
		      (log-message 'message
				   ,(sconc "creating "
					   plural-name-name
					   " table-enum"))
		      (sqlite:execute-non-query db sql))
		    ;; else
		    (unless (string= db-schema sql)
		      (error ,(sconc "mismatch in table schema for sqlite enum "
				     plural-name-name))))
		,@(mapcar (lambda (case)
			    (destructuring-bind (constant id kind)
				case
			      (declare (ignore id))
			      (let ((kind-name (symbol-name* kind)))
				`(let ((db-value (sqlite:execute-single db
									,(format nil "SELECT name FROM ~a WHERE id = ?"
										 sql-name)
									,constant)))
				   (if db-value
				       (unless (string= db-value ,kind-name)
					 (error ,(sconc "mismatch when verifying sqlite enum "
							plural-name-name
							" for kind "
							kind-name
							"(~a in db)")
						db-value))
				       ;; else
				       (progn
					 (log-message 'message
						      ,(sconc "inserting table enum "
							      plural-name-name
							      " kind: "
							      kind-name))
					 (sqlite:execute-non-query db
								   ,(format nil "INSERT INTO ~a (id, name) VALUES (?, ?)"
									    sql-name)
								   ,constant
								   ,kind-name)))))))
			  cases))))
       (register-table-creator ,creator))))

;; ========================================================
;;
;; ========================================================

(defun create-all-tables (db log package)
  (let* ((package (find-package package))
	 (creators (gethash package *sqlite-tables*)))
    (dolist (creator (reverse creators))
      (funcall creator db log))))

;; ========================================================
;;
;; ========================================================


