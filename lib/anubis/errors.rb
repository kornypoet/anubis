module Anubis

  Error                  = Class.new(StandardError)  

  ConnectionError        = Class.new(Error)

  NonexistentTableError  = Class.new(Error)
  NonexistentColumnError = Class.new(Error) 

end
