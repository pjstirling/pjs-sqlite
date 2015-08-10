(in-package #:pjs-sqlite)

(defparameter *sqlite-tables*
  (make-hash-table))

(defparameter *sqlite-table-dependencies*
  (make-hash-table))

(defun create-sqlite-tables-helper (db symbols)
  (dolist (sym symbols)
    (let ((fn (gethash sym *sqlite-tables*)))
      (funcall fn db))))

(defmacro create-sqlite-tables (db &rest tables)
  `(create-sqlite-tables-helper ,db ',tables))

(defun column-is-auto-key-p (col)
  (member :auto-key col))

(defun column-is-multi-key-p (col)
  (member :pk col))

(defun column-is-key-p (col)
  (or (column-is-auto-key-p col)
      (column-is-multi-key-p col)))

(defun column-is-bool-p (col)
  (member :b col))

(defun column-is-integer-p (col)
  (member :i col))

(defun column-is-text-p (col)
  (member :t col))

(defun column-is-unique-p (col)
  (member :unique col))

(defun column-is-nullable-p (col)
  (or (member :n col)
      (column-is-bool-p col)
      (column-is-auto-key-p col)))

(defun column-default-value (col)
  (first-after :default col))
	   
(defun column-is-optional-p (col)
  (or (column-is-auto-key-p col)
      (column-default-value col)))

(defun optional-columns (columns)
  (remove-if-not #'column-is-optional-p columns))

(defun required-columns (columns)
  (remove-if #'column-is-optional-p columns))

(defun column-is-legal-p (col)
  (only-one-of-p col '(:b :i :t :auto-key)))

(defun column-references-table (col)
  (first-after :fk col))

(defun column-predicate (column)
  (symb (first column) '-p))

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

(defun table-dependencies (name columns)
  (remove-if (lambda (col)
	       (or (null col)
		   (eq col name)
		   (and (consp col)
			(eq (first col) name))))
	     (mapcar #'column-references-table
		     columns)))
  
(defun build-create-table-sql (name columns)
  (with-output-to-string (s)
    (format s 
	    "CREATE TABLE ~a (~%    ~a~%)"
	    (sql-name name)
	    (join +separator-string+
		  (mapcar #'(lambda (col)
			      (unless (column-is-legal-p col)
				(error "illegal column spec: ~a" col))
			      (sconc (sql-name (first col))
				     (cond
				       ((column-is-integer-p col)
					" INTEGER")
				       ((column-is-text-p col)
					" TEXT")
				       ((column-is-bool-p col)
					" CHARACTER(1) DEFAULT 'N'")
				       ((column-is-auto-key-p col)
					" INTEGER PRIMARY KEY NOT NULL")
				       (t 
					(error "unhandled column kind (~A)" col)))
				     (awhen (column-default-value col)
				       (format nil " DEFAULT ~w" it))
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
			  columns)
		  (when (find-if #'column-is-multi-key-p
				 columns)
		    (sconc "PRIMARY KEY ( "
			   (join ", "
				 (mapcar #'(lambda (col) (sql-name (first col)))
					 (remove-if-not #'column-is-multi-key-p
							columns)))
			   " )"))))))

(defun create-table-function-name (name)
  (symb 'create- name '-table))

(defun create-table-sql-name (name)
  (symb '+create- name '-sql+))

(defun temporary-table-name (name)
  (sconc (sql-name name) "_temp"))

(defun table-schema-from-db (db name)
  (sqlite:execute-single db (sconc "SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = '"
				   (sql-name name)
				   "'")))

(defun defsqlite-table-creator (name columns)
  `(defun ,(create-table-function-name name) (db recursive log)
     (when (member ',name recursive)
       (error "table reference loop ~a" recursive))
     
     (push ',name recursive)
     
     (let (dep-changed)
       (dolist (table ',(table-dependencies name columns))
	 (setf dep-changed (or dep-changed
			       (funcall (gethash table *sqlite-tables*)
					db
					recursive
					log))))
       
       (let* ((table-schema (sqlite:execute-single db ,(sconc "SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = '"
							      (sql-name name)
							      "'")))
	      (need-to-migrate (and table-schema
				    (string/= table-schema 
					      ,(create-table-sql-name name)))))
	 (when (or (not table-schema)
		   need-to-migrate
		   dep-changed)
	   (with-recursive-transaction db
	     (when need-to-migrate
	       (format t ,(sconc "TABLE SQL FOR '"
				       (sql-name name)
				       "' DIFFERS, ATTEMPTING MIGRATION~%")))
		   
		   (when table-schema
		     (format t "Old schema was: ~w~%New schema is: ~w~%" table-schema ,(create-table-sql-name name))
		     (with-foreign-keys-disabled db
		       (sqlite:execute-non-query db ,(sconc "ALTER TABLE " 
							    (sql-name name)
							    " RENAME TO " (temporary-table-name name)))))
		   
		   (sqlite:execute-non-query db ,(create-table-sql-name name))		   
		   ,@(mapcar (lambda (col)
			       `(create-index db ,name ,(first col)))
			     (remove-if-not #'column-references-table
					    columns))
		   
		   (when table-schema
		     (if (migrate-table-data db ,(temporary-table-name name) ,(sql-name name))
			 (progn
			   (when need-to-migrate
			     (funcall log 'message ,(sconc "AUTOMATED MIGRATION SUCCEEDED, APPARENTLY~%"))
			     
			     ;; (maphash (lambda (table dependencies)
			     ;; 		(when (member ',name dependencies)
			     ;; 		  (funcall (gethash table *sqlite-tables*)
			     ;; 			   db
			     ;; 			   nil)))
			     ;; 	      *sqlite-table-dependencies*)
			     
			     (sqlite:execute-non-query db ,(sconc "DROP TABLE "
								  (temporary-table-name name)))))
			 ;; else
			 (progn
			   (funcall log 'error "AUTOMATED MIGRATION FAILED, HERE BE DRAGONS!~%")
			   (error ,(sconc "can't migrate table " (sql-name name)))))
		     ;; db was changed in some way
		     t)))))))

(defun defsqlite-table-inserter (name columns)
  (let ((required-columns (remove-if #'column-is-optional-p columns))
	(optional-columns (remove-if-not #'column-is-optional-p columns)))
    `(defun ,(symb 'insert- name) (db 
				   ,@(mapcar #'first required-columns)
				   &key ,@(mapcar #'(lambda (col)
						      (list (first col)
							    nil
							    (symb (first col) '-p)))
						  optional-columns))
       (with-sqlite-statements (db
				 (stmt (sconc "INSERT INTO "
					      ,(sql-name name)
					      " ( "
					      (join +separator-string+
						    ,(join +separator-string+
							   (mapcar #'(lambda (x)
								       (sql-name (first x)))
								   required-columns))
						    ,@(mapcar #'(lambda (col)
								  `(when ,(symb (first col) '-p)
								     ,(sql-name (first col))))
							      optional-columns))
					      " ) VALUES ( "
					      (join ","
						    ,(join ","
							   (n-copies (length required-columns)
								     "?"))
						    ,@(mapcar #'(lambda (col)
								  `(when ,(symb (first col) '-p)
								     "?"))
							      optional-columns))
					      " )")))
	 ;; loop for each column, bind-parameter starts at 1
	 ,@(let ((index 1))
	     (mapcar #'(lambda (col)
			 (column-bind name col (1++ index)))
		     required-columns))
	 ,(when optional-columns
	    `(let ((index ,(1+ (length required-columns))))
	       ,@(mapcar #'(lambda (col)
			     `(when ,(symb (first col) '-p)
				,(column-bind name col 'index)
				(incf index)))
			 optional-columns)))
	 ;; stmt from preamble
	 (sqlite:step-statement stmt)
	 (sqlite:reset-statement stmt)
	 (sqlite:last-insert-rowid db)))))

(defun defsqlite-table-updater (name columns)
  (let ((key-columns (remove-if-not #'column-is-key-p columns))
	(non-key-columns (remove-if #'column-is-key-p columns)))
    (if (and key-columns non-key-columns)
	`(defun ,(symb 'update- name) (db
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
								  non-key-columns))
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
	     (let ((index 1))
	       ,@(mapcar (lambda (col)
			   `(when ,(column-predicate col)
			      ,(column-bind name col '(1++ index))))
			 non-key-columns)
	       ,@(mapcar (lambda (col)
			   (column-bind name col '(1++ index)))
			 key-columns)
	       (sqlite:step-statement stmt))))
	;; else
	(format *error-output*
		"~&not generating update-~a because it has no ~a~%" name (if key-columns
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
	`(defun ,(symb 'delete- name) (db ,@(mapcar #'first key-columns))
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

(defmacro defsqlite-table (name &body columns)
  `(progn
     (defparameter ,(create-table-sql-name name)
       ,(build-create-table-sql name columns))
     
     
     ,(defsqlite-table-creator name columns)
     
     (eval-when (:load-toplevel)
       (setf (gethash ',name *sqlite-tables*) #',(create-table-function-name name))
       
       ;; so we can rebuild dependant tables after migration
       (setf (gethash ',name *sqlite-table-dependencies*)
	     ',(table-dependencies name columns)))
     
     ,(defsqlite-table-inserter name columns)
     ,(defsqlite-table-updater name columns)
     ,(defsqlite-table-deleter name columns)))

(defun create-all-tables (db log package)
  (let ((package (find-package package)))
    (maphash (lambda (table fn)
	       (when (eq (symbol-package table)
			 package)
		 (funcall fn db nil log)))
	     *sqlite-tables*)))

;; ========================================================
;;
;; ========================================================

(defun sort-tables ()
  (let (sorted
	tables)
    (maphash (lambda (key value)
	       (push (cons key value)
		     tables))
	     *sqlite-table-dependencies*)
    (while tables
      (block outer
	(dolist (table tables)
	  (when (null (rest table))
	    (let ((table-name (first table)))
	      (setf tables (remove table tables))
	      (push table-name sorted)
	      (setf tables (mapcar (lambda (table)
				     (remove table-name table))
				   tables))
	      (return-from outer))))
	(error "reached end of block without finding a no-dependencies table: ~w" tables)))
    (nreverse sorted)))

(defun remove-whitespace (str)
  (let ((result (make-array (length str) :element-type 'character :fill-pointer 0)))
    (dotimes (i (length str))
      (let ((ch (char str i)))
	(unless (member ch '(#\Space #\Tab #\Return #\Newline))
	  (vector-push ch result))))
    result))

(defun munge-tables (db)
  (let ((sorted (sort-tables)))
    (sqlite:with-transaction db
      (dolist (table sorted)
	(let* ((table-name (sql-name table))
	       (create-table-sql (symbol-value (create-table-sql-name table)))
	       (table-schema (table-schema-from-db db table)))
	  (unless (string= (remove-whitespace create-table-sql)
			   (remove-whitespace table-schema))
	    (format t "schema mis-match:~%  OLD: ~w~%  NEW: ~W~%" table-schema create-table-sql))
	  (when table-schema
	    (sqlite:execute-non-query db (sconc "ALTER TABLE " table-name " RENAME TO " (temporary-table-name table))))
	  (sqlite:execute-non-query db create-table-sql)
	  (when table-schema
	    (migrate-table-data db (temporary-table-name table) table-name))
	  (unless table-schema
	    (sqlite:execute-non-query db (sconc "CREATE TABLE " (temporary-table-name table) " AS SELECT * FROM " table-name)))))
      (dolist (table (reverse sorted))
	(sqlite:execute-non-query db (sconc "DROP TABLE " (temporary-table-name table)))))))
