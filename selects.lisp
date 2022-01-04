(in-package #:pjs-sqlite)

(defun wrap-query (form param-binds interpolate)
  `(sconc "("
	  ,(query form
		  param-binds
		  interpolate)
	  ")"))

(defmacro with-query-parts ((distinct results others) query &body body)
  `(progn
     (unless (eq (first ,query)
		 :select)
       (error "bad select form ~a" ,query))
     (destructuring-bind (,results &rest ,others)
	 (rest ,query)
       (let (,distinct)
	 (when (eq :distinct ,results)
	   (setf ,distinct :distinct)
	   (setf ,results (pop ,others)))
	 ,@body))))

(defun query (form param-binds interpolate)
  (with-query-parts (distinct results clauses)
		    form
    `(sconc "SELECT "
	    ,(when distinct
	       "DISTINCT ")
	    (join ", "
		  ,@ (mapcar (lambda (form)
			       (expr form param-binds interpolate))
			     results))
	    ,@ (mapcar (lambda (clause)
			 (ecase (first clause)
			   (:from
			    `(sconc " FROM "
				    (join ", "
					  ,@ (mapcar (lambda (form)
						       (table-or-query form param-binds interpolate))
						     (rest clause)))))
			   (:where
			    `(sconc " WHERE "
				    ,(expr (second clause)
					   param-binds
					   interpolate)))
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
					   param-binds
					   interpolate)))
			   (:limit
			    `(sconc " LIMIT "
				    ,(expr (second clause)
					   param-binds
					   interpolate)))))
		       clauses))))

(defun table-or-query (form param-binds interpolate)
  (if (symbolp form)
      (sql-name form)
      ;; else
      (ecase (first form)
	(:as
	 `(sconc ,(table-or-query (third form)
				  param-binds
				  interpolate)
		 " AS "
		 ,(sql-name (second form))))
	(:select
	  (wrap-query form
		      param-binds
		      interpolate))
	(:left-join
	 (let ((sources (rest form))
	       result)
	   (push (table-or-query (pop sources)
				 param-binds
				 interpolate)
		 result)
	   (while sources
	     (push " LEFT JOIN "
		   result)
	     (push (table-or-query (pop sources)
				   param-binds
				   interpolate)
		   result)
	     (push " ON "
		   result)
	     (push (expr (pop sources)
			 param-binds
			 interpolate)
		   result))
	   `(sconc ,@ (nreverse result)))))))

(defun expr (form param-binds interpolate)
  (flet ((interpolate-value (place)
	   (if interpolate
	       `(etypecase ,place
		  (number
		   (princ-to-string ,place))
		  (string
		   (sql-quote-string ,place)))
	       ;; else
	       "?")))
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
			   param-binds
			   interpolate)
		    " AS "
		    ,(sql-name name))))
	 ((+ - * / = and or < > >= <=)
	  `(join ,(sconc " "
			 (symbol-name (first form))
			 " ")
		 ,@ (mapcar (lambda (form)
			      (expr form
				    param-binds
				    interpolate))
			    (rest form))))
	 ((when)
	  (destructuring-bind (test body)
	      (rest form)
	    (let* ((saved (length param-binds))
		   (compiled `(when ,test
				,(expr body
				       param-binds
				       interpolate))))
	      (do ((p (length param-binds) (1- p)))
		  ((= p saved))
		(symbol-macrolet ((it (aref param-binds (1- p))))
		  (setf it
			`(when ,test
			   ,it))))
	      compiled)))
	 ((null)
	  `(sconc ,(expr (second form)
			 param-binds
			 interpolate)
		  " IS NULL"))
	 (:var
	   (vector-push-extend form param-binds)
	   (interpolate-value (second form)))
	 (:null=
	  (let ((name (sql-name (second form))))
	    (vector-push-extend form param-binds)
	    `(if ,(third form)
		 (sconc ,name
			" = "
			,(interpolate-value (third form)))
		 ;; else
		 ,(sconc name " IS NULL"))))
	 (:and=
	  (expr `(and ,@ (mapcar (lambda (form)
				   `(:null= ,form ,form))
				 (rest form)))
		param-binds
		interpolate))
	 (:like
	  `(sconc ,(expr (second form)
			 param-binds
			 interpolate)
		  " LIKE "
		  ,(expr (third form)
			 param-binds
			 interpolate)))
	 (:exists
	  `(sconc "EXISTS ("
		  ,(query (second form)
			  param-binds
			  interpolate)
		  ")"))
	 (t
	  `(sconc ,(symbol-name (first form))
		  "("
		  (join ", "
			,@ (mapcar (lambda (form)
				     (expr form
					   param-binds
					   interpolate))
				   (rest form)))
		  ")")))))))

(defun parse-select-form (sql)
  (let (names)
    (flet ((arr ()
	     (make-array 1 :fill-pointer 0 :adjustable t))
	   (rewrite-results ()
	     (with-query-parts (distinct results others)
			       sql
	       `(:select
		  ,@ (when distinct
		       '(:distinct))
		  ,(mapcar (lambda (result)
			     (if (listp result)
				 (destructuring-bind (a name thing)
				     result
				   (assert (eq a :as))
				   (push name names)
				   thing)
				 ;; else
				 (progn
				   (push result names)
				   result)))
			   results)
		  ,@others))))
      (let* ((param-binds (arr))
	     (rewritten (rewrite-results))
	     (sql-forms (query rewritten
			       param-binds
			       nil)))
	(values
	 (nreverse names)
	 sql-forms
	 param-binds
	 (query rewritten
		(arr)
		t))))))

(defun bind-parameters-form (stmt param-binds)
  (let (forms
	(index 0)
	(len (length param-binds)))
    (while (and (< index len)
		(eq :var
		    (first (aref param-binds index))))
      (let ((param (aref param-binds index)))
	(push `(sqlite:bind-parameter ,stmt ,(1+ index) ,(second param))
	      forms)
	(incf index)))
    (cond
      ((< index len)
       (let ((run-index (gensym "PARAMETER-INDEX-"))
	     (first-dynamic index))
	 (labels ((handle-bind (param)
		    (ecase (first param)
		      (:var
			`((sqlite:bind-parameter ,stmt ,run-index ,(second param))
			  (incf ,run-index)))
		      (:null=
		       `((when ,(third param)
			   (sqlite:bind-parameter ,stmt ,run-index ,(third param))
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

(defun query-setup-skeleton (db sql-forms bind-forms stmt body)
  `(with-sqlite-statements (,db (,stmt ,sql-forms))
     ,@ (bind-parameters-form stmt
			      bind-forms)
     ,body))

(defmacro with-statement-results (stmt results &body body)
  `(let ,(loop #:for i #:from 0
	       #:for result #:in results
	       #:collect `(,result (sqlite:statement-column-value ,stmt ,i)))
     ,@body))

(defmacro do-sqlite-query* (db sql-form &body body)
  (let ((stmt (gensym)))
    (unless (and (listp sql-form)
		 (eq (first sql-form)
		     :select))
      (error "funny sql-form ~w" sql-form))
    (multiple-value-bind (results sql-forms bind-forms)
	(parse-select-form sql-form)
      (query-setup-skeleton db sql-forms bind-forms stmt
			    `(while (sqlite:step-statement ,stmt)
			       (with-statement-results ,stmt ,results
				 ,@body))))))

#+nil
(do-sqlite-query* db
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

(defmacro if-sqlite-query (db query yes no)
  (let ((stmt (gensym)))
    (multiple-value-bind (results sql-forms bind-forms interpolated)
	(parse-select-form query)
      (query-setup-skeleton db sql-forms bind-forms stmt
			    `(if (sqlite:step-statement ,stmt)
				 (with-statement-results ,stmt ,results
				   (prog1
				       ,yes
				     (when (sqlite:step-statement ,stmt)
				       (error "unexpected extra row ~a" ,interpolated))))
				 ;; else
				 ,no)))))

(defmacro with-one-sqlite-row (db query &body body)
  (multiple-value-bind (results sql-forms bind-forms interpolated)
      (parse-select-form query)
    (declare (ignore results sql-forms bind-forms))
    `(if-sqlite-query ,db ,query
		      (progn
			,@body)
		      ;; else
		      (error "no row matched ~a" ,interpolated))))



(defmacro with-sqlite-queries* ((db &body queries) &body body)
  `(flet ,(mapcar (lambda (query)
		    (bind ((:db (name sql)
				query)
			   (:mv (results sql-forms- bind-forms)
				(parse-select-form sql)))
		      `(,name ,(map 'list #'second bind-forms)
			      (with-one-sqlite-row ,db
				  ,sql
				(values ,@results)))))
	   queries)
     ,@body))
