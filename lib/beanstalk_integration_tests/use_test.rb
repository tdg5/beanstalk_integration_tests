require 'beanstalk_integration_tests/test_helper'

class UseTest < BeanstalkIntegrationTest

  context 'use' do

    should 'use the default tube by default' do
      assert_equal 'default', client.transmit('list-tube-used')[:id]
    end


    should 'create and use the tube supplied' do
      response = client.transmit("use #{tube_name}")
      assert_equal 'USING', response[:status]
      assert_equal tube_name, response[:id]
      assert_equal(tube_name, client.transmit('list-tube-used')[:id], 'Expected list-tube-used to match used tube name')
      assert(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected used tube to be included in list-tubes')
    end


    should 'increment/decrement the current-using counter on the tube stats' do
      client.transmit("use #{tube_name}")
      initial_using_count = client.transmit("stats-tube #{tube_name}")[:body]['current-using'].to_i
      assert_equal 1, initial_using_count
      other_client = build_client
      other_client.transmit("use #{tube_name}")
      assert_equal initial_using_count + 1, client.transmit("stats-tube #{tube_name}")[:body]['current-using'].to_i
      other_client.transmit("use #{uuid}")
      assert_equal initial_using_count, client.transmit("stats-tube #{tube_name}")[:body]['current-using'].to_i
    end


    should 'increment the current-tubes and cmd-use counters on the server stats' do
      stats = client.transmit('stats')[:body]
      initial_use_count = stats['cmd-use'].to_i
      initial_tube_count = stats['current-tubes'].to_i
      client.transmit("use #{tube_name}")
      stats = client.transmit('stats')[:body]
      assert_equal initial_use_count + 1, stats['cmd-use'].to_i
      assert_equal initial_tube_count + 1, stats['current-tubes'].to_i
    end


    should 'be able to handle tube name of maximum length' do
      name = 'x' * 200
      response = client.transmit("use #{name}")
      assert_equal 'USING', response[:status]
      assert_equal name, response[:id]
    end


    should 'return BAD_FORMAT if tube name > 200 bytes' do
      initital_cmd_use = client.transmit('stats')[:body]['cmd-use']
      assert_raises Beaneater::BadFormatError do
        client.transmit("use #{'x' * 201}")
      end
      assert_equal(initital_cmd_use, client.transmit('stats')[:body]['cmd-use'], 'Expected cmd-use to be unchanged')
    end


    should 'return BAD_FORMAT if line has a trailing space or extraneous args' do
      initital_cmd_use = client.transmit('stats')[:body]['cmd-use']
      [' ', ' foo'].each do |trailer|
        assert_raises Beaneater::BadFormatError do
          client.transmit("use #{tube_name}#{trailer}")
        end
      end
      assert_equal(initital_cmd_use, client.transmit('stats')[:body]['cmd-use'], 'Expected cmd-use to be unchanged')
    end


    should 'return BAD_FORMAT if tube name includes invalid characters or starts with hyphen' do
      initital_cmd_use = client.transmit('stats')[:body]['cmd-use']
      valid_chars = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a + %w[- + / ; . $ _ ( )] + [' ']
      # Beanstalk hangs on char 13
      bad_chars = (0..12).to_a.concat((14..255).to_a).to_a.map!(&:chr) - valid_chars

      assert_raises(Beaneater::BadFormatError, 'Expected tube name starting with hyphen to be invalid') do
        client.transmit('use -starts-with-hyphen')
      end
      assert_equal(initital_cmd_use, client.transmit('stats')[:body]['cmd-use'], 'Expected cmd-use to be unchanged')

      bad_chars.each do |bad_char|
        assert_raises(Beaneater::BadFormatError, "Expected tube name with char #{bad_char} to be invalid") do
          client.transmit("use name_with_#{bad_char}_in_it")
        end
      end
      assert_equal(initital_cmd_use, client.transmit('stats')[:body]['cmd-use'], 'Expected cmd-use to be unchanged')
    end


    should 'delete tubes that are not watched and are no longer used' do
      client.transmit("use #{tube_name}")
      tube_stats = client.transmit("stats-tube #{tube_name}")
      assert_equal 1, tube_stats[:body]['current-using']
      assert_equal 0, tube_stats[:body]['current-watching']
      client.transmit("use #{generate_tube_name}")
      assert_raises(Beaneater::NotFoundError, 'Expected unused tube to be deleted') do
        client.transmit("stats-tube #{tube_name}")
      end
      refute(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected unused tube to not be included in list-tubes')
    end


    should 'delete tubes that are not watched and are no longer used even if paused' do
      client.transmit("use #{tube_name}")
      tube_stats = client.transmit("stats-tube #{tube_name}")
      assert_equal 1, tube_stats[:body]['current-using']
      assert_equal 0, tube_stats[:body]['current-watching']
      client.transmit("pause-tube #{tube_name} 120")
      client.transmit("use #{generate_tube_name}")
      assert_raises(Beaneater::NotFoundError, 'Expected unused tube to be deleted') do
        client.transmit("stats-tube #{tube_name}")
      end
      refute(client.transmit('list-tubes')[:body].include?(tube_name), 'Expected unused tube to not be included in list-tubes')
    end

  end

end
