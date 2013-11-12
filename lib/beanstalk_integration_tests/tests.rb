require File.expand_path('../../beanstalk_integration_tests', __FILE__)
Dir.glob(File.expand_path('../**/*_test.rb', __FILE__)).each do |test_file|
  require test_file
end
