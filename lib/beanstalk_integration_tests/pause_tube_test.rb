require 'test_helper'

class PauseTubeTest < BeanstalkIntegrationTest

  context 'pause-tube' do

    setup do
      client.transmit("watch #{tube_name}")
      client.transmit("use #{tube_name}")
    end


    should 'pause the provided tube for the provided delay' do
      pause = 120
      initial_tube_cmd_pause = client.transmit("stats-tube #{tube_name}")[:body]['cmd-pause-tube']
      response = client.transmit("pause-tube #{tube_name} #{pause}")
      assert_equal 'PAUSED', response[:status]
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal pause, tube_stats['pause']
      assert(tube_stats['pause-time-left'] < pause, 'Expected tube pause-time-left to be less than total pause time')
      assert_equal(initial_tube_cmd_pause + 1, tube_stats['cmd-pause-tube'], 'Expected tube cmd-pause-tube to be incremented')
    end


    should 'activate a tube after the timeout has expired' do
      pause = 1
      initial_tube_cmd_pause = client.transmit("stats-tube #{tube_name}")[:body]['cmd-pause-tube']
      response = client.transmit("pause-tube #{tube_name} #{pause}")
      assert_equal 'PAUSED', response[:status]
      sleep 1
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal(0, tube_stats['pause'], 'Expected tube pause to reset after pause expired')
      assert_equal(0, tube_stats['pause-time-left'], 'Expected tube pause-time-left to reset after pause expired')
      assert_equal(initial_tube_cmd_pause + 1, tube_stats['cmd-pause-tube'], 'Expected tube cmd-pause-tube to be incremented')
    end


    should 'honor reservations as soon as pause expires' do
      message = uuid
      client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")
      client.transmit("pause-tube #{tube_name} 1")

      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert(tube_stats['pause'] != 0, 'Expected tube to be paused')
      assert_raises(Beaneater::TimedOutError, 'Expected reserve to fail while tube paused') do
        client.transmit('reserve-with-timeout 0')
      end
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert(tube_stats['pause'] != 0, 'Expected tube to be paused')
      begin
        job = client.transmit('reserve-with-timeout 2')
      rescue Beaneater::TimedOutError => e
        puts 'Tube did not honor reservation in expected period following pause'
        raise e
      end
      assert_equal(message, job[:body], 'Did not reserve expected job')
      client.transmit("delete #{job[:id]}")
    end


    should 'allow reservations while paused, but not reply' do
      client.transmit("pause-tube #{tube_name} 120")

      Thread.new do
        other_client = build_client
        other_client.transmit("watch #{tube_name}")
        assert_raises('Beaneater::TimedOutError', 'Expected waiting for paused tube to timeout') do
          other_client.transmit('reserve-with-timeout 2')
        end
      end

      sleep 0.5
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal(1, tube_stats['current-waiting'])
    end


    should 'accept jobs while paused' do
      client.transmit("pause-tube #{tube_name} 120")
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert(tube_stats['pause'] != 0, 'Expected tube to be paused')
      message = uuid
      client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")
      assert_equal(1, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-ready'], 'Expected paused tube to accept put commands')
    end


    should 'return BAD_FORMAT without trailing space, tube name, or pause' do
      ['', ' ', " #{tube_name}"].each do |trailer|
        assert_raises(Beaneater::BadFormatError, 'Expected pause-tube without space/tube name to return BAD_FORMAT') do
          client.transmit("pause-tube#{trailer}")
        end
      end
    end


    should 'be able to handle tube name of maximum length' do
      name = 'c' * 200
      response = client.transmit("watch #{name}")
      response = client.transmit("pause-tube #{name} 10")
      assert_equal 'PAUSED', response[:status]
      tube_stats = client.transmit("stats-tube #{name}")[:body]
      assert(tube_stats['pause'] != 0, 'Expected tube to be paused')
    end


    should 'return BAD_FORMAT if tube name > 200 bytes' do
      assert_raises Beaneater::BadFormatError do
        client.transmit("pause-tube #{'x' * 201} 10")
      end
    end


    should 'return BAD_FORMAT if line has a trailing space' do
      initital_cmd_pause_tube = client.transmit("stats-tube #{tube_name}")[:body]['cmd-pause-tube']
      assert_raises Beaneater::BadFormatError do
        client.transmit("pause-tube #{tube_name} 10 ")
      end
      assert_equal(initital_cmd_pause_tube, client.transmit("stats-tube #{tube_name}")[:body]['cmd-pause-tube'], 'Expected cmd-pause-tube to be unchanged')
    end


    should 'return BAD_FORMAT if tube name includes invalid characters or starts with hyphen' do
      valid_chars = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a + %w[- + / ; . $ _ ( )] + [' ']
      # Beanstalk hangs on char 13
      bad_chars = (0..12).to_a.concat((14..255).to_a).to_a.map!(&:chr) - valid_chars

      assert_raises(Beaneater::BadFormatError, 'Expected tube name starting with hyphen to be invalid') do
        client.transmit('pause-tube -starts-with-hyphen 10')
      end

      bad_chars.each do |bad_char|
        assert_raises(Beaneater::BadFormatError, "Expected tube name with char #{bad_char} to be invalid") do
          client.transmit("pause-tube name_with_#{bad_char}_in_it 10")
        end
      end
    end

  end

end
