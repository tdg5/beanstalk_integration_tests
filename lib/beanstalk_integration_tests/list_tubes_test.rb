require 'beanstalk_integration_tests/test_helper'

class ListTubesTest < BeanstalkIntegrationTest

  # Checking if tubes are correctly added/removed is handled by UseTest and WatchTest

  context 'list-tubes' do

    should 'should update cmd-list-tubes' do
      initial_cmd_list_tubes = client.transmit('stats')[:body]['cmd-list-tubes']
      client.transmit('list-tubes')
      assert_equal(initial_cmd_list_tubes + 1, client.transmit('stats')[:body]['cmd-list-tubes'], 'Expected cmd-list-tubes to be incremented')
    end


    should 'return bad_format with trailing space or args' do
      [' ', ' trailing_arg'].each do |trailer|
        assert_raises(Beaneater::BadFormatError, 'Expected list-tubes with trailer to return BAD_FORMAT') do
          client.transmit("list-tubes#{trailer}")
        end
      end
    end

  end

end
