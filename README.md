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
