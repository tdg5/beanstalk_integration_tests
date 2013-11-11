# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'beanstalk_integration_tests/version'

Gem::Specification.new do |spec|
  spec.name          = 'beanstalk_integration_tests'
  spec.version       = BeanstalkIntegrationTests::VERSION
  spec.authors       = ['Danny Guinther']
  spec.email         = ['dannyguinther@gmail.com']
  spec.description   = %q{Integration tests for testing adherence to the beanstalkd protocol}
  spec.summary       = %q{Beanstalk protocol integration tests}
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^lib/beanstalk_integration_tests/test/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'beaneater'
  spec.add_development_dependency 'minitest_should'
  spec.add_development_dependency 'mocha'
end
