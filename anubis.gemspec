$:.unshift File.expand_path('../lib', __FILE__)
require 'anubis/version'

Gem::Specification.new do |s|
  s.name          = 'anubis'
  s.version       = Anubis::VERSION
  s.authors       = ['Travis Dempsey']
  s.email         = ['travis@infochimps.com']
  s.homepage      = 'https://github.com/kornypoet/anubis'
  s.summary       = 'An HBase Thrift client written in Ruby'
  s.description   = <<-DESC.gsub(/^ {4}/, '')
    As Stargate provides a REST client for interacting with HBase, Anubis provides a set of Ruby bindings utilizing the built-in Thrift server, for interacting with HBase.
    All of the methods provided by the REST client are implemented (and then some).

    The name comes from the (classic) 1994 Kurt Russel film Stargate, which is heavily inspired by Egyptian mythology. As Anubis is one of the gods featured in the film, 
    his name was chosen to be an alternative to Stargate.
  DESC
  s.files         = `git ls-files`.split("\n").reject{ |f| f =~ /gemspec/ }
  s.require_paths = ['lib']
  
  s.add_dependency('thrift', '~> 0.9')
end
