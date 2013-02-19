# Anubis

[![Build Status](https://travis-ci.org/kornypoet/anubis.png)](https://travis-ci.org/kornypoet/anubis)

As Stargate provides a REST client for interacting with HBase, Anubis provides a set of Ruby bindings utilizing the built-in Thrift server, for interacting with HBase. All of the methods provided by the REST client are implemented (and then some).

The name comes from the (classic) 1994 Kurt Russel film Stargate, which is heavily inspired by Egyptian mythology. As Anubis is one of the gods featured in the film, the name was chosen to be an alternative to Stargate.

## Installation

From the commandline:

`$ gem install anubis`

Or, if you are using Bundler, add this to your `Gemfile`:

```ruby
gem 'anubis'
```

## Development

To work on this gem, clone the repository from github and install the dependencies:

```
$ git clone git@github.com:kornypoet/anubis.git
$ cd anubis/
$ bundle
```

To run the tests:

`$ bundle exec rake`

To install the gem locally:

`$ bundle exec rake install`

## Usage

First, configure the Anubis connection:

```ruby
# By default, without configuration, Anubis will attempt to connect to localhost at port 9090	
Anubis.configure do |c|
  c.host = 'your.moms.server'
  c.port = 9090	  
end
```

Make sure you connect to HBase before creating tables or inserting rows:

```ruby
Anubis.connect!
```

To see a list of the tables:

```ruby
Anubis.tables	
```

Anubis comes with a `Table` model with easy to use dsl methods:

```ruby
t = Anubis::Table.find_or_create 'my_table', 'column_name'
#=> <Anubis::Table[ my_table ] => columns["column_name"]>
t.exists?
#=> true
t.describe
#=> {:name=>"my_table", :columns=>[{:name=>"column_name", :versions=>3, :compression=>"NONE", :in_memory=>false, :ttl=>-1, :cached=>false, :bloom_filter=>{:type=>"NONE", :vector_size=>0, :hashes=>0}}]}
t.delete
#=> true
t.exists?
#=> false
```

## Operations

Operations are possible on a `Table` or on the top-level `Anubis` module:

```ruby
t = Anubis::Table.find_or_create 'my_table', 'column_name', 'another_column'
op = t.columns(:another_column).qualifer(:my_qualifier).rows('my:row:key')
op.put 'some_value'
op.get
#=> {"foo:bar"=>[{:column=>"column_name:", :value=>"some_value", :timestamp=>1361293380609}]}
```

Operations are built upon rows, which are created from a cross-product of row_key and full column names. When you create an operation from a table, the table name and column families are selected by default.

```ruby
t = Anubis::Table.find_or_create 'my_table', 'column_name', 'another_column'
op = t.qualifer(:my_qualifier).rows('my:row:key')
#=> Op: my_table | [ my:row:key ] x [ column_name:my_qualifier, another_column:my_qualifier ]
op.get
```
