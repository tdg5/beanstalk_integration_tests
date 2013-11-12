require 'beanstalk_integration_tests/test_helper'

class ReleaseTest < BeanstalkIntegrationTest

  context 'release with a reserved job' do

    setup do
      client.transmit("use #{tube_name}")
      client.transmit("watch #{tube_name}")
      message = uuid
      client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")
      timeout(2) do
        @reserved_id = client.transmit('reserve')[:id]
      end
      stats = client.transmit('stats')[:body]
      @initial_cmd_release = stats['cmd-release']
      @initial_server_jobs_urgent = stats['current-jobs-urgent']
      @initial_server_jobs_ready = stats['current-jobs-ready']
      @initial_server_jobs_delayed = stats['current-jobs-delayed']
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      @initial_tube_jobs_urgent = tube_stats['current-jobs-urgent']
      @initial_tube_jobs_ready = tube_stats['current-jobs-ready']
      @initial_tube_jobs_delayed = tube_stats['current-jobs-delayed']
      @initial_job_releases = client.transmit("stats-job #{@reserved_id}")[:body]['releases']
    end


    should 'release a job reserved by the client' do
      release_response = client.transmit("release #{@reserved_id} 0 0")
      assert_equal 'RELEASED', release_response[:status]
      assert_equal 'ready', client.transmit("stats-job #{@reserved_id}")[:body]['state']
      assert_counter_delta
    end


    should 'release a job reserved by the client when job deadline is pending' do
      message = uuid
      client.transmit("delete #{@reserved_id}")
      client.transmit("put 0 0 1 #{message.bytesize}\r\n#{message}")
      timeout(2) do
        @reserved_id = client.transmit('reserve')[:id]
      end
      assert_raises(Beaneater::DeadlineSoonError, 'Expected reserve to raise an error when deadline pending') do
        timeout(2) do
          client.transmit('reserve')
        end
      end
      release_response = client.transmit("release #{@reserved_id} 0 0")
      assert_equal 'RELEASED', release_response[:status]
      assert_equal 'ready', client.transmit("stats-job #{@reserved_id}")[:body]['state']
      assert_counter_delta
    end


    should 'release a job to the original tube' do
      release_response = client.transmit("release #{@reserved_id} 0 0")
      assert_equal 'RELEASED', release_response[:status]
      assert_equal tube_name, client.transmit("stats-job #{@reserved_id}")[:body]['tube']
    end


    should 'release a job with the provided delay' do
      delay = 25
      release_response = client.transmit("release #{@reserved_id} 0 #{delay}")
      assert_equal 'RELEASED', release_response[:status]
      job_stats = client.transmit("stats-job #{@reserved_id}")[:body]
      assert_equal delay, job_stats['delay']
      assert_equal 'delayed', job_stats['state']
      assert_counter_delta(1, 'delayed')
    end


    should 'release a job with the provided priority' do
      pri = 25
      release_response = client.transmit("release #{@reserved_id} #{pri} 0")
      assert_equal 'RELEASED', release_response[:status]
      job_stats = client.transmit("stats-job #{@reserved_id}")[:body]
      assert_equal pri, job_stats['pri']
    end


    should 'return a job released with a delay to ready after timeout' do
      delay = 1
      release_response = client.transmit("release #{@reserved_id} 0 #{delay}")
      assert_equal 'RELEASED', release_response[:status]
      job_stats = client.transmit("stats-job #{@reserved_id}")[:body]
      assert_equal delay, job_stats['delay']
      assert_equal 'delayed', job_stats['state']
      assert_counter_delta(1, 'delayed')
      sleep 1.1
      assert_equal 'ready', client.transmit("stats-job #{@reserved_id}")[:body]['state']
      assert_counter_delta
    end


    should 'not be able to release a job reserved by another client' do
      initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
      assert_raises(Beaneater::NotFoundError, 'Expected releaing a job reserved by another client to raise an error') do
        build_client.transmit("release #{@reserved_id} 0 0")
      end
      assert_equal(initial_cmd_release + 1, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be incremented')
      client.transmit("delete #{@reserved_id}")
    end


    should 'not be able to release an unreserved job' do
      initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
      initial_job_releases = client.transmit("stats-job #{@reserved_id}")[:body]['releases']
      client.transmit("release #{@reserved_id} 0 0")
      assert_raises(Beaneater::NotFoundError, 'Expected releaing an unreserved job to raise an error') do
        client.transmit("release #{@reserved_id} 0 0")
      end
      assert_equal(initial_cmd_release + 2, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be incremented')
      assert_equal(initial_job_releases + 1, client.transmit("stats-job #{@reserved_id}")[:body]['releases'], 'Expected job releases to be unchanged')
    end


    should 'return BAD_FORMAT with negative delay or priority' do
      args = [-1, 1]
      initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
      initial_job_releases = client.transmit("stats-job #{@reserved_id}")[:body]['releases']
      args.length.times do
        assert_raises(Beaneater::BadFormatError, 'Expected release with negative argument to raise BAD_FORMAT') do
          client.transmit(['release', @reserved_id].concat(args).join(' '))
        end
        assert_equal(initial_cmd_release, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release to be unchanged')
        assert_equal(initial_job_releases, client.transmit("stats-job #{@reserved_id}")[:body]['releases'], 'Expected job releases to be unchanged')
        args.cycle
      end
      client.transmit("delete #{@reserved_id}")
    end

  end


  context 'release not requiring a reserved job' do

    should 'return BAD_FORMAT with trailing space or arguments' do
      initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
      assert_raises(Beaneater::BadFormatError, 'Expected release with trailing space to return BAD_FORMAT') do
        client.transmit('release 1 0 0 ')
      end
      assert_equal(initial_cmd_release, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be unchanged')

      assert_raises(Beaneater::BadFormatError, 'Expected release with trailing space to return BAD_FORMAT') do
        client.transmit('release 1 0 0 x')
      end
      assert_equal(initial_cmd_release, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be unchanged')
    end


    should 'return BAD_FORMAT with non-integer job_id, delay, or priority' do
      args = ['foo', 1, 1]
      args.length.times do
        initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
        assert_raises(Beaneater::BadFormatError, 'Expected release with non-integer argument to raise BAD_FORMAT') do
          client.transmit(['release'].concat(args).join(' '))
        end
        assert_equal(initial_cmd_release, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be unchanged')
        args.cycle
      end
    end


    should 'return NOT_FOUND with negative job_id' do
      initial_cmd_release = client.transmit('stats')[:body]['cmd-release']
      assert_raises(Beaneater::NotFoundError, 'Expected release with negative job_id to raise NOT_FOUND') do
        client.transmit('release -1 0 0')
      end
      assert_equal(initial_cmd_release + 1, client.transmit('stats')[:body]['cmd-release'], 'Expected cmd-release stats to be incremented')
    end

  end


  def assert_counter_delta(delta = 1, job_state = 'ready')
    stats = client.transmit('stats')[:body]
    tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
    assert_equal(@initial_cmd_release + delta, stats['cmd-release'], 'Expected cmd-release to be incremented')
    assert_equal(instance_variable_get("@initial_server_jobs_#{job_state}") + delta, stats["current-jobs-#{job_state}"], "Expected server current-jobs-#{job_state} to be incremented")
    assert_equal(instance_variable_get("@initial_tube_jobs_#{job_state}") + delta, tube_stats["current-jobs-#{job_state}"], "Expected tube current-jobs-#{job_state} to be incremented")
    if job_state == 'ready'
      assert_equal(@initial_server_jobs_urgent + delta, stats['current-jobs-urgent'], 'Expected server current-jobs-urgent to be incremented')
      assert_equal(@initial_tube_jobs_urgent + delta, tube_stats['current-jobs-urgent'], 'Expected tube current-jobs-urgent to be incremented')
    end
    assert_equal(@initial_job_releases + delta, client.transmit("stats-job #{@reserved_id}")[:body]['releases'], 'Expected job releases to be incremented')
    assert_equal(0, stats['current-jobs-reserved'], 'Expected server current-jobs-reserved to be zero')
    assert_equal(0, tube_stats['current-jobs-reserved'], 'Expected tube current-jobs-reserved to be zero')
  end

end
