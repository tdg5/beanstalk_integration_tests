require 'beanstalk_integration_tests/test_helper'
require 'socket'
require 'English'

class StatsTest < BeanstalkIntegrationTest

  context 'stats' do

    should 'return server stats with expected keys' do
      expected_keys = %w[
        current-jobs-urgent current-jobs-ready current-jobs-reserved current-jobs-delayed
        current-jobs-buried cmd-put cmd-peek cmd-peek-ready cmd-peek-delayed cmd-peek-buried cmd-reserve
        cmd-reserve-with-timeout cmd-delete cmd-release cmd-use cmd-watch cmd-ignore cmd-bury cmd-kick
        cmd-touch cmd-stats cmd-stats-job cmd-stats-tube cmd-list-tubes cmd-list-tube-used
        cmd-list-tubes-watched cmd-pause-tube job-timeouts total-jobs max-job-size current-tubes
        current-connections current-producers current-workers current-waiting total-connections pid
        version rusage-utime rusage-stime uptime binlog-oldest-index binlog-current-index
        binlog-records-migrated binlog-records-written binlog-max-size id hostname
      ]
      initial_cmd_stats = client.transmit('stats')[:body]['cmd-stats']
      response = client.transmit('stats')
      assert_equal 'OK', response[:status]
      assert_equal expected_keys, response[:body].keys
      assert_nil response[:body].values.compact!, 'Expected stats to include no nil values'
      assert_equal(initial_cmd_stats + 1, response[:body]['cmd-stats'], 'Expected cmd-stats to be incremented')
    end

    # Some stats are tested elsewhere:
    #
    # :current-jobs-urgent => PutTest, ReleaseTest
    # :current-jobs-ready => PutTest, ReleaseTest
    # :current-jobs-reserved => ReserveTest, ReleaseTest
    # :current-jobs-delayed => KickTest, PutTest, ReleaseTest
    # :current-jobs-buried => BuryTest, KickTest
    # :total-jobs => PutTest
    # :cmd-put => PutTest
    # :cmd-peek => PeekTest
    # :cmd-peek-ready => PeekTest
    # :cmd-peek-delayed => PeekTest
    # :cmd-peek-buried => PeekTest
    # :cmd-reserve => ReserveTest
    # :cmd-use => UseTest
    # :cmd-watch => WatchTest
    # :cmd-ignore => IgnoreTest
    # :cmd-delete => DeleteTest
    # :cmd-release => ReleaseTest
    # :cmd-bury => BuryTest
    # :cmd-kick => KickTest
    # :cmd-stats => See above/below
    # :cmd-stats-job => StatsJobTest
    # :cmd-stats-tube => StatsTubeTest
    # :cmd-list-tubes => ListTubesTest
    # :cmd-list-tube-used => ListTubeUsedTest
    # :cmd-list-tubes-watched => ListTubesWatchedTest
    # :cmd-pause-tube => PauseTubeTest
    # :job-timeouts => TouchTest
    # :total-jobs => PutTest
    # :max-job-size => PutTest (Tests retrieval directly, indiectly tests value set correctly)
    # :current-tube => UseTest, Watch Test
    # :current-connections => See below
    # :current-producers => PutTest
    # :current-workers => ReserveTest
    # :current-waiting => ReserveTest
    # :total-connections => See below
    # :pid => See below
    # :version => NOT TESTED only tested for presence
    # :rusage-utime => NOT TESTED only tested for presence
    # :rusage-stime => NOT TESTED only tested for presence
    # :uptime => See below
    # :binlog-oldest-index => NOT TESTED only tested for presence
    # :binlog-current-index => NOT TESTED only tested for presence
    # :binlog-max-size => NOT TESTED only tested for presence
    # :binlog-records-written => NOT TESTED only tested for presence
    # :binlog-records-written => NOT TESTED only tested for presence
    # :id => NOT TESTED only tested for presence
    # :hostname => See below
    #
    # For those that aren't...


    should 'return correct hostname' do
      # This test assume tests and beanstalk are being run on same host
      assert_equal Socket.gethostname, client.transmit('stats')[:body]['hostname']
    end


    should 'return correct uptime' do
      initial_uptime = client.transmit('stats')[:body]['uptime']
      sleep 1
      updated_uptime = client.transmit('stats')[:body]['uptime']
      assert(updated_uptime > initial_uptime, 'Expected uptime to to be incremented')
    end


    should 'return correct count of current-connections and total-connections' do
      stats = client.transmit('stats')[:body]
      initial_current_connections = stats['current-connections']
      initial_total_connections = stats['total-connections']
      other_client = build_client
      stats = client.transmit('stats')[:body]
      assert_equal(initial_current_connections + 1, stats['current-connections'], 'Expected current-connections to be incremented')
      assert_equal(initial_total_connections + 1, stats['total-connections'], 'Expected total-connections to be incremented')
      other_client.close
      assert_equal(stats['current-connections'] - 1, client.transmit('stats')[:body]['current-connections'], 'Expected current-connections to be decremented')
    end


    should 'return BAD_FORMAT with any trailing characters' do
      initial_cmd_stats = client.transmit('stats')[:body]['cmd-stats']
      [' ', ' foo'].each do |trailer|
        assert_raises(Beaneater::BadFormatError, 'Expected stats with any trailing characters to return BAD_FORMAT') do
          client.transmit("stats#{trailer}")
        end
      end
      assert_equal(initial_cmd_stats + 1, client.transmit('stats')[:body]['cmd-stats'], 'Expected cmd-stats to be unchanged')
    end

  end

end
