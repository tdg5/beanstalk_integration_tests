require 'beanstalk_integration_tests/test_helper'

class StatsTubeTest < BeanstalkIntegrationTest

  context 'stats-tube' do

    setup do
      client.transmit("use #{tube_name}")
    end


    should 'return stats for the specified tube with all expected keys' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      response = client.transmit("stats-tube #{tube_name}")
      assert_equal 'OK', response[:status]
      tube_stats = response[:body]
      expected_keys = %w[
        name current-jobs-urgent current-jobs-ready current-jobs-reserved current-jobs-delayed
        current-jobs-buried total-jobs current-using current-watching current-waiting cmd-delete
        cmd-pause-tube pause pause-time-left
      ]
      assert_equal expected_keys, tube_stats.keys
      assert_nil tube_stats.values.compact!, 'Expected tube stats to have no nil values'
      assert_equal(initial_cmd_stats_tube + 1, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be incremented')
    end


    # Some stats are tested elsewhere:
    #
    # :name => See below
    # :current-jobs-urgent => PutTest, ReleaseTest
    # :current-jobs-ready => PutTest, ReleaseTest
    # :current-jobs-reserved => ReserveTest, ReleaseTest
    # :current-jobs-delayed => KickTest, PutTest, ReleaseTest
    # :current-jobs-buried => BuryTest, KickTest
    # :total-jobs => PutTest
    # :current-using => UseTest
    # :current-waiting => ReserveTest
    # :current-watching => IgnoreTest, UseTest, WatchTest
    # :pause => PauseTubeTest
    # :cmd-delete => DeleteTest
    # :cmd-pause-tube => PauseTubeTest
    # :pause-time-left => PauseTubeTest
    #
    # For those that aren't...


    should 'return expected tube name' do
      assert_equal(tube_name, client.transmit("stats-tube #{tube_name}")[:body]['name'], 'Expected stats-tube to return correct tube name')
    end


    should 'return NOT_FOUND if the tube has never existed' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises(Beaneater::NotFoundError, 'Expected tube-stats for non-existent tube to return NOT_FOUND') do
        client.transmit("stats-tube #{uuid}")
      end
      assert_equal(initial_cmd_stats_tube + 1, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be incremented')
    end


    should 'return NOT_FOUND if the tube is deleted' do
      client.transmit('use default')
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises(Beaneater::NotFoundError, 'Expected tube-stats for deleted tube to return NOT_FOUND') do
        client.transmit("stats-tube #{tube_name}")
      end
      assert_equal(initial_cmd_stats_tube + 1, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be incremented')
    end


    should 'return BAD_FORMAT without trailing space' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises(Beaneater::BadFormatError, 'Expected tube-stats without space to return BAD_FORMAT') do
        client.transmit('stats-tube')
      end
      assert_equal(initial_cmd_stats_tube, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')
    end


    should 'return BAD_FORMAT if tube name is null' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises(Beaneater::BadFormatError, 'Expected tube-stats with null tube name to return BAD_FORMAT') do
        client.transmit('stats-tube ')
      end
      assert_equal(initial_cmd_stats_tube, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')
    end


    should 'be able to handle tube name of maximum length' do
      name = 'x' * 200
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      client.transmit("watch #{name}")
      response = client.transmit("stats-tube #{name}")
      assert_equal 'OK', response[:status]
      assert_equal name, response[:body]['name']
      assert_equal(initial_cmd_stats_tube + 1, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be incremented')
    end


    should 'return BAD_FORMAT if tube name > 200 bytes' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises Beaneater::BadFormatError do
        client.transmit("stats-tube #{'x' * 201}")
      end
      # Get stats with other client because long tube name leaves client in weird state
      assert_equal(initial_cmd_stats_tube, build_client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')
    end


    should 'return BAD_FORMAT if line has a trailing space' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      assert_raises Beaneater::BadFormatError do
        client.transmit("stats-tube #{tube_name} ")
      end
      assert_equal(initial_cmd_stats_tube, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')
    end


    should 'return BAD_FORMAT if tube name includes invalid characters or starts with hyphen' do
      initial_cmd_stats_tube = client.transmit('stats')[:body]['cmd-stats-tube']
      valid_chars = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a + %w[- + / ; . $ _ ( )] + [' ']
      # Beanstalk hangs on char 13
      bad_chars = (0..12).to_a.concat((14..255).to_a).to_a.map!(&:chr) - valid_chars

      assert_raises(Beaneater::BadFormatError, 'Expected tube name starting with hyphen to be invalid') do
        client.transmit('stats-tube -starts-with-hyphen')
      end
      assert_equal(initial_cmd_stats_tube, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')

      bad_chars.each do |bad_char|
        assert_raises(Beaneater::BadFormatError, "Expected tube name with char #{bad_char} to be invalid") do
          client.transmit("stats-tube name_with_#{bad_char}_in_it")
        end
      end
      assert_equal(initial_cmd_stats_tube, client.transmit('stats')[:body]['cmd-stats-tube'], 'Expected cmd-stats-tube to be unchanged')
    end

  end

end
