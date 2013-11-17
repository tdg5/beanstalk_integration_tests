require 'beanstalk_integration_tests/test_helper'

class OtherTest < BeanstalkIntegrationTest

  context 'Bad commands' do

    should 'return UNKNOWN_COMMAND for a variery of bad commands' do
      ['', "st\r\nats", '123', 'st ats', ' stats'].each do |command|
        assert_raises(Beaneater::UnknownCommandError, 'Expected bad command to raise UNKNOWN_COMMAND') do
          client.transmit(command)
        end
      end
    end

  end

end
