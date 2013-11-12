require 'beanstalk_integration_tests/test_helper'

class TouchTest < BeanstalkIntegrationTest

  context 'touch with reserved job' do

    should 'reset job time-left to job ttr' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      do_setup(5)
      sleep 1
      assert_equal(@ttr - 2, client.transmit("stats-job #{@reserved_id}")[:body]['time-left'])
      client.transmit("touch #{@reserved_id}")
      assert_equal(@ttr - 1, client.transmit("stats-job #{@reserved_id}")[:body]['time-left'])
      assert_equal(initial_cmd_touch + 1, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
      client.transmit("delete #{@reserved_id}")
    end


    should 'prevent timeout from occuring' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      do_setup(1)
      # Allow initial timeout period to expire quickly
      3.times do
        sleep 0.5
        client.transmit("touch #{@reserved_id}")
      end
      job_stats = client.transmit("stats-job #{@reserved_id}")[:body]
      client.transmit("delete #{@reserved_id}")
      assert_equal('reserved', job_stats['state'])
      assert_equal(0, job_stats['timeouts'])
      assert_equal(initial_cmd_touch + 3, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
    end


    should 'not allow touching jobs reserved by other clients' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      do_setup(10)
      sleep 1
      initial_time_left = client.transmit("stats-job #{@reserved_id}")[:body]['time-left']
      assert_raises(Beaneater::NotFoundError, 'Expected touching a job reserved by another client to return NOT_FOUND') do
        build_client.transmit("touch #{@reserved_id}")
      end
      assert(initial_time_left >= client.transmit("stats-job #{@reserved_id}")[:body]['time-left'], 'Expected time-left to be less after failed touch')
      client.transmit("delete #{@reserved_id}")
      assert_equal(initial_cmd_touch + 1, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
    end


    should 'not allow touching jobs that are not reserved' do
      stats = client.transmit('stats')[:body]
      initial_cmd_touch = stats['cmd-touch']
      initial_job_timeouts = stats['job-timeouts']
      do_setup(1)
      sleep 1
      assert_raises(Beaneater::NotFoundError, 'Expected touching an unreserved job to return NOT_FOUND') do
        build_client.transmit("touch #{@reserved_id}")
      end
      assert_equal(1, client.transmit("stats-job #{@reserved_id}")[:body]['timeouts'], 'Expected job timeouts to be incremented')
      client.transmit("delete #{@reserved_id}")
      stats = client.transmit('stats')[:body]
      assert_equal(initial_cmd_touch + 1, stats['cmd-touch'], 'Expected cmd-touch to be incremented')
      assert_equal(initial_job_timeouts + 1, stats['job-timeouts'], 'Expected job-timeouts to be incremented')
    end


    should 'ignore trailing space, arguments, and chars' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      do_setup(10)
      [' ', ' and ignore these arguments', '_and_ignore_these_letters'].each do |trailer|
        sleep 1
        time_left = client.transmit("stats-job #{@reserved_id}")[:body]['time-left']
        client.transmit("touch #{@reserved_id}#{trailer}")
        assert(client.transmit("stats-job #{@reserved_id}")[:body]['time-left'] > time_left, 'Expected time-left to be greater after touch')
      end
      client.transmit("delete #{@reserved_id}")
      assert_equal(initial_cmd_touch + 3, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
    end

  end


  context 'touch not requiring reserved job' do

    should 'return UNKNOWN_COMMAND if no space after command' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      assert_raises(Beaneater::UnknownCommandError, 'Expected touch with no following space to return UNKNOWN_COMMAND') do
        build_client.transmit('touch')
      end
      assert_equal(initial_cmd_touch, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be unchanged')
    end


    should 'return NOT_FOUND if non-integer or null job_id provided' do
      initial_cmd_touch = client.transmit('stats')[:body]['cmd-touch']
      assert_raises(Beaneater::NotFoundError, 'Expected touch with no job_id to return NOT_FOUND') do
        build_client.transmit('touch ')
      end
      assert_equal(initial_cmd_touch + 1, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
      assert_raises(Beaneater::NotFoundError, 'Expected touch with no job_id to return NOT_FOUND') do
        build_client.transmit('touch x')
      end
      assert_equal(initial_cmd_touch + 2, client.transmit('stats')[:body]['cmd-touch'], 'Expected cmd-touch to be incremented')
    end

  end

  def do_setup(ttr = 5)
    message = uuid
    @ttr = ttr
    client.transmit("watch #{tube_name}")
    client.transmit("use #{tube_name}")
    client.transmit("put 0 0 #{@ttr} #{message.bytesize}\r\n#{message}")
    timeout(1) do
      @reserved_id = client.transmit('reserve')[:id]
    end
  end

end
