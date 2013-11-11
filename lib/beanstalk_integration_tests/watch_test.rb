require 'test_helper'

class WatchTest < BeanstalkIntegrationTest

  context 'watch' do

    should 'watch the default tube by default' do
      assert client.transmit('list-tubes-watched')[:body].include?('default')
    end


    should 'create and watch the tube supplied and return the incremented count of tubes watched' do
      assert_equal 1, client.transmit('list-tubes-watched')[:body].length
      response = client.transmit("watch #{tube_name}")
      assert_equal 'WATCHING', response[:status]
      tubes_watched = client.transmit('list-tubes-watched')[:body]
      assert_equal tubes_watched.length, response[:id].to_i
      assert tubes_watched.include?(tube_name)
      assert(client.transmit('list-tubes-watched')[:body].include?(tube_name), 'Expected watched tube to be included in list-tubes-watched')
      assert(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected watched tube to be included in list-tubes')
    end


    should 'increment the number of watchers on the tubes stats' do
      client.transmit("watch #{tube_name}")
      assert_equal 1, client.transmit("stats-tube #{tube_name}")[:body]['current-watching']
    end


    should 'increment the current-tubes and cmd-watch counters on the server stats' do
      stats = client.transmit('stats')[:body]
      initial_use_count = stats['cmd-watch'].to_i
      initial_tube_count = stats['current-tubes'].to_i
      client.transmit("watch #{tube_name}")
      stats = client.transmit('stats')[:body]
      assert_equal initial_use_count + 1, stats['cmd-watch'].to_i
      assert_equal initial_tube_count + 1, stats['current-tubes'].to_i
    end


    should 'not increment count of tubes watched if tube already watched' do
      initial_cmd_watch = client.transmit('stats')[:body]['cmd-watch']
      assert_equal 1, client.transmit('list-tubes-watched')[:body].length
      response = client.transmit("watch #{tube_name}")
      assert_equal 2, client.transmit('list-tubes-watched')[:body].length
      response = client.transmit("watch #{tube_name}")
      assert_equal 2, client.transmit('list-tubes-watched')[:body].length
      assert_equal(initial_cmd_watch + 2, client.transmit('stats')[:body]['cmd-watch'], 'Expected cmd-watch to be incremented')
    end


    should 'return BAD_FORMAT if line has a trailing space' do
      initital_cmd_use = client.transmit('stats')[:body]['cmd-watch']
      assert_raises Beaneater::BadFormatError do
        client.transmit("watch #{tube_name} ")
      end
      assert_equal(initital_cmd_use, client.transmit('stats')[:body]['cmd-watch'], 'Expected cmd-watch to be unchanged')
    end


    should 'be able to handle tube name of maximum length' do
      response = client.transmit("watch #{'x' * 200}")
      assert_equal 'WATCHING', response[:status]
      assert_equal '2', response[:id]
    end


    should 'return BAD_FORMAT if tube name > 200 bytes' do
      initial_cmd_watch = client.transmit('stats')[:body]['cmd-watch']
      assert_raises Beaneater::BadFormatError do
        client.transmit("watch #{'x' * 201}")
      end
      # Extra long command leaves client in weird place
      assert_equal(initial_cmd_watch, build_client.transmit('stats')[:body]['cmd-watch'], 'Expected cmd-watch to remain unchanged')
    end


    should 'return BAD_FORMAT if tube name includes invalid characters or starts with hyphen' do
      initial_cmd_watch = client.transmit('stats')[:body]['cmd-watch']
      valid_chars = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a + %w[- + / ; . $ _ ( )] + [' ']
      # Beanstalk hangs on char 13
      bad_chars = (0..12).to_a.concat((14..255).to_a).to_a.map!(&:chr) - valid_chars

      assert_raises(Beaneater::BadFormatError, 'Expected tube name starting with hyphen to be invalid') do
        client.transmit('watch -starts-with-hyphen')
      end
      assert_equal(initial_cmd_watch, client.transmit('stats')[:body]['cmd-watch'], 'Expected cmd-watch to remain unchanged')

      bad_chars.each do |bad_char|
        assert_raises(Beaneater::BadFormatError, "Expected tube name with char #{bad_char} to be invalid") do
          client.transmit("watch name_with_#{bad_char}_in_it")
        end
      end
      assert_equal(initial_cmd_watch, client.transmit('stats')[:body]['cmd-watch'], 'Expected cmd-watch to remain unchanged')
    end


    should 'delete tubes that are not being used and are no longer watched' do
      client.transmit("watch #{tube_name}")
      tube_stats = client.transmit("stats-tube #{tube_name}")
      assert_equal 1, tube_stats[:body]['current-watching']
      assert_equal 0, tube_stats[:body]['current-using']
      client.transmit("watch #{generate_tube_name}")
      client.transmit("ignore #{tube_name}")
      assert_raises(Beaneater::NotFoundError, 'Expected unwatched tube to be deleted') do
        client.transmit("stats-tube #{tube_name}")
      end
      refute(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected unwatched tube to not be included in list-tubes')
    end


    should 'delete tubes that are not being used and are no longer watched even if paused' do
      client.transmit("watch #{tube_name}")
      tube_stats = client.transmit("stats-tube #{tube_name}")
      assert_equal 1, tube_stats[:body]['current-watching']
      assert_equal 0, tube_stats[:body]['current-using']
      client.transmit("watch #{generate_tube_name}")
      client.transmit("pause-tube #{tube_name} 120")
      client.transmit("ignore #{tube_name}")
      assert_raises(Beaneater::NotFoundError, 'Expected unwatched tube to be deleted') do
        client.transmit("stats-tube #{tube_name}")
      end
      refute(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected unwatched tube to not be included in list-tubes')
    end

  end

end
