(in-package :pjs-sqlite)

(defparameter *sqlite-transaction-active*
  nil)

(defparameter +separator-string+
  (format nil ",~%   "))

(defmacro with-recursive-transaction (db &body body)
  (let ((fn (gensym)))
    `(flet ((,fn ()
	      ,@body))
       (if *sqlite-transaction-active*
	   (,fn)
	   ;; else
	   (let ((*sqlite-transaction-active* t))
	     (sqlite:with-transaction ,db
	       (,fn)))))))
	       
(defmacro with-sqlite-statements ((db &body stmts) &body body)
  (let ((result `(progn ,@body)))
    ;; reverse it for expected ordering
    (dolist (stmt (reverse stmts))
      (setf result `(let ((,(first stmt) (sqlite:prepare-statement ,db ,(second stmt))))
		      (unwind-protect ,result
			(sqlite:finalize-statement ,(first stmt))))))
    result))

#|
(with-sqlite-statements (db (a "insert foo") (b "select bar"))
  (foobar)
  (if a
      z
      -1)
  (baz))
|#

(defmacro with-foreign-keys-disabled (db &body body)
  (let ((old-value (gensym))
	(db-name (gensym)))
    `(let* ((,db-name ,db)
	    (,old-value (sqlite:execute-single ,db-name "PRAGMA foreign_keys")))
       (unwind-protect
	    (progn
	      (sqlite:execute-non-query ,db-name "PRAGMA foreign_keys=0")
	      ,@body)
	 (sqlite:execute-non-query ,db-name (format nil "PRAGMA foreign_keys=~w" ,old-value))))))

(defun fetch-table-column-names (db name)
  (with-sqlite-statements (db
			   (stmt (sconc "SELECT * FROM "
					name)))
    (sqlite:resultset-columns-names stmt)))
      
(defun migrate-table-data (db source dest)
  (let ((column-names (intersection (fetch-table-column-names db source)
				    (fetch-table-column-names db dest)
				    :test #'string=)))
    (unless column-names
      (error "table ~a and table ~a have no columns in common!" source dest))
    
    (multiple-value-bind (result condition)
	(ignore-errors
	  (sqlite:execute-non-query db
				    (sconc "INSERT INTO "
					   dest
					   " ( "
					   (join ", "
						 column-names)
					   " ) SELECT "
					   (join ", "
						 column-names)
					   " FROM "
					   source)))
      (declare (ignore result))
      (not condition))))
      
(defun create-index-helper (db index-name sql)
  (let ((schema (sqlite:execute-single db "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = ?" index-name)))
    (unless (and schema
		 (string= schema 
			  sql))
      (sqlite:execute-non-query db (sconc "DROP INDEX IF EXISTS " index-name))
      (sqlite:execute-non-query db sql))))

(defmacro create-index (db table &rest columns)
  (let* ((table-name (sql-name table))
	 (sql-columns (mapcar #'sql-name columns))
	 (index-name (sconc table-name
			    "_"
			    (apply #'join "_" sql-columns)
			    "_index"))
	 (sql (sconc "CREATE INDEX " index-name " ON " table-name 
		     " ( " (join ", " sql-columns) " )")))
    `(create-index-helper ,db ,index-name ,sql)))
					 
(defmacro do-sqlite-query (db (sql &rest args) (&rest fields) &body body)
  "Creates a prepared statement using SQL, binds the parameters in ARGS and then loops over the result-set binding the columns in-turn to FIELDS for each row."
  (let ((stmt (gensym)))
    `(with-sqlite-statements (,db (,stmt ,sql))
       ;; bind-parameter is 1 based
       ,@(let ((counter 1))
	   (mapcar #'(lambda (x)
		       `(sqlite:bind-parameter ,stmt ,(1++ counter) ,x))
		   args))
       (while (sqlite:step-statement ,stmt)
	 ;; statement-column-value is 0 based
	 (let ,(let ((counter 0))
		     (mapcar #'(lambda (x)
				 `(,x (sqlite:statement-column-value ,stmt ,(1++ counter))))
			     fields))
	   ,@body)))))

#|
(do-sqlite-query db ("SELECT id, track_name FROM tracks WHERE artist_id = ?" artist-id) (track-id track-name)
  (format t "~a ~a" track-id track-name))
|#

(defmacro do-sqlite-query* (db (sql args) (&rest fields) &body body)
  "Like DO-SQLITE-QUERY but uses a runtime ARGS list which skips NIL elements."
  (let ((stmt (gensym))
	(counter (gensym))
	(arg (gensym))
	(field-index 0))
    `(with-sqlite-statements (,db (,stmt ,sql))
       (let ((,counter 1))
	 (dolist (,arg ,args)
	   (when ,arg
	     (sqlite:bind-parameter ,stmt ,counter ,arg)
	     (incf ,counter))))
       (while (sqlite:step-statement ,stmt)
	 (let ,(mapcar (lambda (field)
			 (prog1
			     `(,field (sqlite:statement-column-value ,stmt
								     ,field-index))
			   (incf field-index)))
		fields)
	   ,@body)))))

(defun sqlite-execute-to-flat-list (db sql &rest args)
  (with-sqlite-statements (db (stmt sql))
    (let ((counter 1)
	  result)
      (dolist (arg args)
	(sqlite:bind-parameter stmt counter arg)
	(incf counter))

      (while (sqlite:step-statement stmt)
	(push (sqlite:statement-column-value stmt 0)
	      result))
      (nreverse result))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun sql-name (name)
    (let ((str (or (and (stringp name)
			name)
		   (and (symbolp name)
			(symbol-name name))
		   (error "bad name for sql-name ~w" name))))
      (with-output-to-string (s)
	(dovector (ch str)
	  (cond
	    ((eql ch #\%)
	     (princ "percent_" s))
	    ((eql ch #\-)
	     (princ #\_ s))
	    ((eql ch #\/)
	     (princ "_slash_" s))
	    (t
	     (princ (char-downcase ch) s))))))))

(defmacro sql-and-equal-string (&rest names)
  `(join+ " AND "
	  ,@(mapcar (lambda (name)
		      (let ((sql-name (sql-name name)))
			`(if ,name
			     ,(sconc sql-name " = ?")
			     ;; else
			     ,(sconc sql-name " IS NULL"))))
		    names)))

(defun bool-col (col)
  (if (and col
	   (not (equalp col "N")))
      "Y"
      ;; else
      "N"))

;; this helper lets you use constructs like
;; (OR col-name ...) in addition to simple
;; col-name
(defun magic-column-arg-name (arg)
  (sql-name (if (listp arg)
		(second arg)
		arg)))

;; ======================================================
;; uses name of the variable for the name of the column
;; to test whether the data exists in the table, and
;; returns the value of 'id' if so
;; ======================================================

(defmacro key-in-table (db table &rest known-args)
  (let ((syms (mapcar #'(lambda (col)
			  (list (gensym) col))
		      known-args)))
    `(let ,syms
       (with-sqlite-statements (,db
				(stmt (sconc ,(sconc "SELECT id FROM "
						     (sql-name table)
						     " WHERE ")
					     (join+ " AND "
						    ,@(mapcar #'(lambda (arg)
								  (let ((arg-name (magic-column-arg-name (second arg))))
								    `(if ,(first arg)
									 ,(sconc arg-name " = ?")
									 ,(sconc arg-name " IS NULL"))))
							      syms)))))
	 (let ((index 0))
	   ,@(mapcar #'(lambda (arg)
			 `(when ,(first arg)
			    (sqlite:bind-parameter stmt (incf index) ,(first arg))))
		     syms)
	   (if (sqlite:step-statement stmt)
	       (sqlite:statement-column-value stmt 0)
	       nil))))))

;; ===================================================
;; return the value of 'id' for the row with the 
;; values passed, or inserts a new row
;; ===================================================

(defmacro select-or-insert (db table &rest known-args)
  `(let* (,@(mapcar #'(lambda (x)
			(let ((arg-name (if (listp x)
					    (second x)
					    x)))
			  `(,arg-name ,x)))
		    known-args)
	  (id (key-in-table ,db ,table ,@known-args)))
     (if id
	 id
	 ;; else
	 (progn
	   (sqlite:execute-non-query ,db 
				     ,(sconc "INSERT INTO "
					     (sql-name table)
					     " ( "
					     (join ", "
						   (mapcar #'magic-column-arg-name known-args))
					     " ) VALUES ( "
					     (join ", "
						   (n-copies (length known-args)
							     "?"))
					     " )")
				     ,@known-args)
	   (sqlite:last-insert-rowid ,db)))))

(defclass query-clause ()
  ((func-name :initarg :name
	      :reader query-clause-func-name)
   (stmt :reader query-clause-stmt)
   (column-count-name :reader query-clause-column-count-name)
   (sql :initarg :sql
	:reader query-clause-sql)))


(defmethod initialize-instance :after ((obj query-clause) &key name)
  (setf (slot-value obj 'stmt)
	(gensymb name "-stmt")
	(slot-value obj 'column-count-name)
	(gensymb name "-column-count")))

(defmacro with-sqlite-queries ((db &rest clauses) &body body)
  (let ((clauses (mapcar (lambda (clause)
			   (make-instance 'query-clause
					  :name (first clause)
					  :sql (second clause)))
			 clauses)))
    `(with-sqlite-statements (,db
			      ,@(mapcar (lambda (clause)
					  (list (query-clause-stmt clause)
						(query-clause-sql clause)))
					clauses))
       (let ,(mapcar #'query-clause-column-count-name clauses)
	 (flet (,@(mapcar (lambda (clause)
			    (let* ((col-count (query-clause-column-count-name clause))
				   (stmt (query-clause-stmt clause))
				   (name (query-clause-func-name clause))
				   (vars (count #\? (query-clause-sql clause)))
				   (args (dotimes-c (var vars)
					   (collect (symb 'arg var)))))
			      `(,name ,args
			          (sqlite:reset-statement ,stmt)
				  ,@(let ((index 1))
				    (mapcar (lambda (var)
					      `(sqlite:bind-parameter ,stmt ,(1++ index) ,var))
						  args))
				  (sqlite:step-statement ,stmt)
				  (unless ,col-count
				    (setf ,col-count (length (sqlite:statement-column-names ,stmt))))
				  (values-list
				   (dotimes-c (i ,col-count)
				     (collect (sqlite:statement-column-value ,stmt i)))))))
			  clauses))
	   ,@body)))))

(defmacro create-with-db-macro (name db path)
  `(defmacro ,name (&body body)
     `(sqlite:with-open-database (,',db ,',path :busy-timeout 5000)
	(sqlite:execute-non-query ,',db "PRAGMA foreign_keys=1")
	,@body)))
