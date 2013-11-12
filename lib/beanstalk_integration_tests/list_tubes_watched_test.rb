require 'beanstalk_integration_tests/test_helper'

class ListTubesWatchedTest < BeanstalkIntegrationTest

  # Checking if tubes are correctly added/removed is handled by WatchTest and IgnoreTest

  context 'list-tubes-watched' do

    should 'should update cmd-list-tubes-watched' do
      initial_cmd_list_tubes_watched = client.transmit('stats')[:body]['cmd-list-tubes-watched']
      client.transmit('list-tubes-watched')
      assert_equal(initial_cmd_list_tubes_watched + 1, client.transmit('stats')[:body]['cmd-list-tubes-watched'], 'Expected cmd-list-tubes-watched to be incremented')
    end


    should 'return bad_format with trailing space or args' do
      [' ', ' trailing_arg'].each do |trailer|
        assert_raises(Beaneater::BadFormatError, 'Expected list-tubes-watched with trailer to return BAD_FORMAT') do
          client.transmit("list-tubes-watched#{trailer}")
        end
      end
    end

  end

end
