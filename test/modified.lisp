(defpackage #:pjs-sqlite-tests
  (:use #:cl #:pjs-sqlite #:pjs-utils)
  (:export #:run-tests))

(in-package #:pjs-sqlite-tests)

(defun log-message (datum fmt &rest args)
  (format t "[~a] ~a~%" datum (apply #'format nil fmt args)))

(defmacro with-test (name (&body tables) &body body)
  (let ((test-fn (symb name "-test-fn")))
    `(progn
       ,@tables
       (defun ,test-fn ()
	 (let* ((template ,(format nil "/tmp/~a.sqlite.XXXXXXXXX" name)))
	   (sb-posix:mkstemp template)
	   (unwind-protect
		(sqlite:with-open-database (db template :busy-timeout 500)
		  ,@ (mapcar (lambda (table)
			       (let* ((table-name (second table))
				      (creator-name (pjs-sqlite::create-table-function-name table-name)))
				 `(,creator-name db #'log-message)))
			     tables)
		  ,@body)
	     (delete-file (sb-ext:parse-native-namestring template)))))
       (,test-fn))))

(with-test "modified"
    ((defsqlite-table modified-test
	(id :auto-key)
	(name :t)
	(last :modified)))
  (let ((id (insert-modified-test db "initial")))
    (with-one-sqlite-row db
	(:select ((:as original-last last))
	  (:from modified-test)
	  (:where (= id (:var id))))
      (sleep 5)
      (update-modified-test db
			    id
			    :name "updated")
      (with-one-sqlite-row db
	  (:select (id last)
	    (:from modified-test)
	    (:where (= id (:var id))))
	(unless (string/= original-last last)
	  (error "failed test ~a ~a" original-last last))))))
