(in-package #:pjs-sqlite)

(defun wrap-query (form param-binds)
  `(sconc "("
	  ,(query form param-binds)
	  ")"))

(defun query (form param-binds)
  (unless (eq :select (first form))
    (error "dodgy query form ~w" form))
  (let* ((clauses (rest form))
	 (results (pop clauses))
	 distinct)
    (when (eq :distinct results)
      (setf distinct t)
      (setf results (pop clauses)))
    (values
     `(sconc "SELECT "
	     ,(when distinct
		"DISTINCT ")
	     (join ", "
		   ,@ (mapcar (lambda (form)
				(expr form param-binds))
			      results))
	     ,@ (mapcar (lambda (clause)
			  (ecase (first clause)
			    (:from
			     `(sconc " FROM "
				     (join ", "
					   ,@ (mapcar (lambda (form)
							(table-or-query form param-binds))
						      (rest clause)))))
			    (:where
			     `(sconc " WHERE "
				     ,(expr (second clause)
					    param-binds)))
			    (:order-by
			     `(sconc " ORDER BY "
				     (join ", "
					   ,@ (mapcar (lambda (form)
							(if (listp form)
							    (destructuring-bind (ordering term)
								form
							      (sconc (sql-name term)
								     " "
								     (symbol-name ordering)))
							    ;; else
							    (sql-name form)))
						      (rest clause)))))
			    (:group-by
			     `(sconc " GROUP BY "
				     ,(expr (second clause)
					    param-binds)))
			    (:limit
			     `(sconc " LIMIT "
				     ,(expr (second clause)
					    param-binds)))))
			clauses))
     results)))

(defun table-or-query (form param-binds)
  (if (symbolp form)
      (sql-name form)
      ;; else
      (ecase (first form)
	(:as
	 `(sconc ,(table-or-query (third form)
				  param-binds)
		 " AS "
		 ,(sql-name (second form))))
	(:select
	  (wrap-query form
		      param-binds))
	(:left-join
	 (let ((sources (rest form))
	       result)
	   (push (table-or-query (pop sources)
				 param-binds)
		 result)
	   (while sources
	     (push " LEFT JOIN "
		   result)
	     (push (table-or-query (pop sources)
				   param-binds)
		   result)
	     (push " ON "
		   result)
	     (push (expr (pop sources)
			 param-binds)
		   result))
	   `(sconc ,@ (nreverse result)))))))

(defun expr (form param-binds)
  (etypecase form
    (symbol
     (sql-name form))
    (number
     (princ-to-string form))
    (string
     (sql-quote-string form))
    (cons
     (case (first form)
       (:as
	(destructuring-bind (name val)
	    (rest form)
	  `(sconc ,(expr val
			 param-binds)
		  " AS "
		  ,(sql-name name))))
       ((+ - * / = and or < > >= <=)
	`(join ,(sconc " "
		       (symbol-name (first form))
		       " ")
	       ,@ (mapcar (lambda (form)
			    (expr form param-binds))
			  (rest form))))
       ((when)
	(destructuring-bind (test body)
	    (rest form)
	  (let* ((saved (length param-binds))
		 (compiled `(when ,test
			      ,(expr body param-binds))))
	    (do ((p (length param-binds) (1- p)))
		((= p saved))
	      (symbol-macrolet ((it (aref param-binds (1- p))))
		(setf it
		      `(when ,test
			 ,it))))
	    compiled)))
       ((null)
	`(sconc ,(expr (second form) param-binds)
		" IS NULL"))
       (:var
	 (vector-push-extend form param-binds)
	 "?")
       (:null=
	(let ((name (sql-name (second form))))
	  (vector-push-extend form param-binds)
	  `(if ,(third form)
	       ,(sconc name " = ?")
	       ;; else
	       ,(sconc name " IS NULL"))))
       (:and=
	(expr `(and ,@ (mapcar (lambda (form)
				 `(:null= ,form ,form))
			       (rest form)))
	      param-binds))
       (:like
	`(sconc ,(expr (second form)
		       param-binds)
		" LIKE "
		,(expr (third form)
		       param-binds)))
       (:exists
	`(sconc "EXISTS ("
		,(query (second form)
			param-binds)
		")"))
       (t
	`(sconc ,(symbol-name (first form))
		"("
		(join ", "
		      ,@ (mapcar (lambda (form)
				   (expr form param-binds))
				 (rest form)))
		")"))))))

(defun parse-select-form (sql)
  (let ((param-binds (make-array 1 :fill-pointer 0 :adjustable t)))
    (multiple-value-bind (sql-forms results)
	(query sql param-binds)
      (values (mapcar (lambda (result)
			(if (listp result)
			    (progn
			      (unless (eq (first result)
					  :as)
				(error "funny result ~a" result))
			      (second result))
			    ;; else
			    result))
		      results)
	      sql-forms
	      param-binds))))

(defparameter *enable-statement-debugging*
  nil)

(defparameter *statement-debugging-parameters*
  nil)

(defmacro bind-parameter (stmt index value)
  (if *enable-statement-debugging*
      (bind ((:symbols stmt-place index-place value-place))
	`(let* ((,stmt-place ,stmt)
		(,index-place ,index)
		(,value-place ,value))
	   (sqlite:bind-parameter ,stmt-place
				  ,index-place
				  ,value-place)
	   (push (list ,index-place
		       ,value-place)
		 *statement-debugging-parameters*)))
      ;; else
      `(sqlite:bind-parameter ,stmt ,index ,value)))

(defun bind-parameters-form (stmt param-binds)
  (let (forms
	(index 0)
	(len (length param-binds)))
    (while (and (< index len)
		(eq :var
		    (first (aref param-binds index))))
      (let ((param (aref param-binds index)))
	(push `(bind-parameter ,stmt ,(1+ index) ,(second param))
	      forms)
	(incf index)))
    (cond
      ((< index len)
       (let ((run-index (gensym "PARAMETER-INDEX-"))
	     (first-dynamic index))
	 (labels ((handle-bind (param)
		    (ecase (first param)
		      (:var
			`((bind-parameter ,stmt ,run-index ,(second param))
			  (incf ,run-index)))
		      (:null=
		       `((when ,(third param)
			   (bind-parameter ,stmt ,run-index ,(third param))
			   ,(when param-binds
			      `(incf ,run-index)))))
		      ((when)
		       `((when ,(second param)
			   ,@ (handle-bind (third param))))))))
	   (while (< index len)
	     (dolist (form (handle-bind (aref param-binds index)))
	       (push form forms))
	     (incf index)))
	 `((let ((,run-index ,(1+ first-dynamic)))
	     ,@ (nreverse forms)))))
      (t
       (nreverse forms)))))

(defmacro do-sqlite-query* (db sql-form &body body)
  (let ((stmt (gensym)))
    (unless (and (listp sql-form)
		 (eq (first sql-form)
		     :select))
      (error "funny sql-form ~w" sql-form))
    (multiple-value-bind (results sql-forms bind-forms)
	(parse-select-form sql-form)
      `(with-sqlite-statements (,db (,stmt ,sql-forms))
	 ,@ (bind-parameters-form stmt
				  bind-forms)
	 ,@ (when *enable-statement-debugging*
	      `((dump-interpolated-statement ,stmt)))
	 (while (sqlite:step-statement ,stmt)
	   (let ,(loop #:for i #:from 0
		       #:for result #:in results
		       #:collect `(,result (sqlite:statement-column-value ,stmt ,i)))
	     ,@body))))))

#+nil
(do-sqlite-query% db
  (:select
    (track_name
     artist_id
     artist_name
     album_id
     album_name
     track_number
     track_count
     disc_number
     disc_count
     disc_name
     disc_id
     track_year
     gapless
     compilation)
    (:from
     (:left-join
      tracks
      discs
      (= tracks.disc-id
	 discs.id)
      albums
      (= discs.album-id
	 albums.id)
      artists
      (= tracks.artist-id
	 artists.id)))
    (:where
     (:null= tracks.id id)))
  foo)

(defun dump-interpolated-statement (stmt)
  (let* ((parts (explode "?"
			 (sqlite::sql stmt)))
	 (result (pop parts))
	 (highest (apply #'max
			 (mapcar #'first
				 *statement-debugging-parameters*)))
	 (vec (make-array highest)))
    (dolist (el *statement-debugging-parameters*)
      (setf (aref vec (1- (first el)))
	    (second el)))
    (dovector (el vec)
      (setf result
	    (sconc result
		   (if (stringp el)
		       (sconc "'"
			      (apply #'join
				     "''"
				     (explode "'" el))
			      "'")
		       ;; else
		       (format nil "~w" el))
		   (pop parts))))
    (setf *statement-debugging-parameters*
	  nil)
    (princ result)
    (princ #\;)
    (terpri)))
