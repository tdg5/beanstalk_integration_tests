# BeanstalkIntegrationTests

A suite of integration tests to test adherence to the Beanstalkd protocol (https://github.com/kr/beanstalkd/blob/master/doc/protocol.md).

Some tests will fail at present (as of version 1.9+9+g157d88b or https://github.com/kr/beanstalkd/commit/157d88bf9435a23b71a1940a9afb617e52a2b9e9). I maybe incorrect, but I believe these are related to previously undiagnosed bugs in the current version of beanstalkd.


## Installation

Add this line to your application's Gemfile:

    gem 'beanstalk_integration_tests'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install beanstalk_integration_tests


## Usage

Start your beanstalkd server on 127.0.0.1:11300 or localhost:11300

Run integration tests:

    $ rake


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
