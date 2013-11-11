require 'test_helper'

class IgnoreTest < BeanstalkIntegrationTest

  context 'ignore' do

    context 'when a tube is watched' do

      setup do
        client.transmit("watch #{tube_name}")

        # Use a second client to keep tube open
        @other_client = build_client
        @other_client.transmit("watch #{tube_name}")
      end


      should 'ignore the provided tube' do
        initial_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
        initial_tube_watching = client.transmit("stats-tube #{tube_name}")[:body]['current-watching']
        assert(client.transmit('list-tubes-watched')[:body].include?(tube_name), 'Expected watched tube to be listed in list-tubes-watched')
        response = client.transmit("ignore #{tube_name}")
        assert_equal 'WATCHING', response[:status]
        assert_equal '1', response[:id]
        refute(client.transmit('list-tubes-watched')[:body].include?(tube_name), 'Expected ignore to remove tube from list-tubes-watched')
        assert_equal(initial_cmd_ignore + 1, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be incremented')
        assert_equal(initial_tube_watching - 1, client.transmit("stats-tube #{tube_name}")[:body]['current-watching'], 'Expected tube current-watching to be decremented')

        @other_client.transmit("ignore #{tube_name}")
        assert_equal(initial_cmd_ignore + 2, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be incremented')
        assert_raises(Beaneater::NotFoundError, 'Expected unwatched tube to be destroyed') do
          client.transmit("stats-tube #{tube_name}")
        end
        refute(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected ignore to remove tube from list-tubes')
      end


      should 'not ignore the provided tube if no other tubes are watched' do
        initial_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
        initial_tube_watching = client.transmit("stats-tube #{tube_name}")[:body]['current-watching']
        client.transmit('ignore default')
        assert_raises(Beaneater::NotIgnoredError, 'Expected ignoring last watched tube to return NOT_IGNORED') do
          client.transmit("ignore #{tube_name}")
        end
        assert(client.transmit('list-tubes-watched')[:body].include?(tube_name), 'Expected watched tube to be listed in list-tubes-watched')
        assert_equal(initial_cmd_ignore + 2, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be incremented')
        assert_equal(initial_tube_watching, client.transmit("stats-tube #{tube_name}")[:body]['current-watching'], 'Expected tube current-watching to be unchanged')
      end


      should 'return count of tubes watched when ignoring unwatched or non-existent tube' do
        %w[unwatched non-existant].each do |name|
          initial_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
          response = client.transmit("ignore #{name}")
          assert_equal 'WATCHING', response[:status]
          assert_equal '2', response[:id]
          assert_equal(initial_cmd_ignore + 1, client.transmit('stats')[:body]['cmd-ignore'], "Expected cmd-ignore to be incremented when ignoring #{name} tube")
        end
      end


      should 'return UNKNOWN_COMMAND if command does not include space' do
        assert_raises(Beaneater::UnknownCommandError, 'Expected ignore without space to return UNKNOWN_COMMAND') do
          client.transmit('ignore')
        end
      end


      should 'return BAD_FORMAT if tube name is not provided' do
        assert_raises(Beaneater::BadFormatError, 'Expected ignore without tube name to return BAD_FORMAT') do
          client.transmit('ignore ')
        end
      end


      should 'be able to handle tube name of maximum length' do
        name = 'c' * 200
        initial_watch_count = client.transmit('list-tubes-watched')[:body].length
        client.transmit("watch #{name}")
        response = client.transmit("ignore #{name}")
        assert_equal 'WATCHING', response[:status]
        assert_equal initial_watch_count.to_s, response[:id]
      end


      should 'return BAD_FORMAT if tube name > 200 bytes' do
        initital_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
        assert_raises Beaneater::BadFormatError do
          client.transmit("ignore #{'x' * 201}")
        end
        # Use other client because extra long tube name leaves client in weird state
        assert_equal(initital_cmd_ignore, build_client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be unchanged')
      end


      should 'return BAD_FORMAT if line has a trailing space' do
        initital_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
        assert_raises Beaneater::BadFormatError do
          client.transmit("ignore #{tube_name} ")
        end
        assert_equal(initital_cmd_ignore, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be unchanged')
      end


      should 'return BAD_FORMAT if tube name includes invalid characters or starts with hyphen' do
        initital_cmd_ignore = client.transmit('stats')[:body]['cmd-ignore']
        valid_chars = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a + %w[- + / ; . $ _ ( )] + [' ']
        # Beanstalk hangs on char 13
        bad_chars = (0..12).to_a.concat((14..255).to_a).to_a.map!(&:chr) - valid_chars

        assert_raises(Beaneater::BadFormatError, 'Expected tube name starting with hyphen to be invalid') do
          client.transmit('ignore -starts-with-hyphen')
        end
        assert_equal(initital_cmd_ignore, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be unchanged')

        bad_chars.each do |bad_char|
          assert_raises(Beaneater::BadFormatError, "Expected tube name with char #{bad_char} to be invalid") do
            client.transmit("ignore name_with_#{bad_char}_in_it")
          end
        end
        assert_equal(initital_cmd_ignore, client.transmit('stats')[:body]['cmd-ignore'], 'Expected cmd-ignore to be unchanged')
      end
    end

  end

end
