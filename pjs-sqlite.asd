(eval-when (:compile-toplevel :load-toplevel)
  (quicklisp:quickload "sqlite"))

(asdf:defsystem #:pjs-sqlite
  :depends-on (#:pjs-utils #:sqlite)
  :serial t
  :components ((:file "package")
               (:file "sqlite")
	       (:file "tables")))
