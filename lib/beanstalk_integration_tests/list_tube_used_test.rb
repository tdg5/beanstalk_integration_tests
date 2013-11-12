require 'beanstalk_integration_tests/test_helper'

class ListTubeUsedTest < BeanstalkIntegrationTest

  # Checking if tube is correctly maintained is handled by UseTest

  context 'list-tube-used' do

    should 'should update cmd-list-tube-used' do
      initial_cmd_list_tube_used = client.transmit('stats')[:body]['cmd-list-tube-used']
      client.transmit('list-tube-used')
      assert_equal(initial_cmd_list_tube_used + 1, client.transmit('stats')[:body]['cmd-list-tube-used'], 'Expected cmd-list-tube-used to be incremented')
    end


    should 'return bad_format with trailing space or args' do
      [' ', ' trailing_arg'].each do |trailer|
        assert_raises(Beaneater::BadFormatError, 'Expected list-tubes with trailer to return BAD_FORMAT') do
          client.transmit("list-tube-used#{trailer}")
        end
      end
    end

  end

end
