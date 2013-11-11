require 'bundler/gem_tasks'
require 'rake/testtask'

Rake::TestTask.new do |t|
  t.libs << 'lib/beanstalk_integration_tests'
  t.pattern = 'lib/beanstalk_integration_tests/**/*_test.rb'
end

task :default => :test
