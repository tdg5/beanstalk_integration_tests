require 'beanstalk_integration_tests/test_helper'

class BuryTest < BeanstalkIntegrationTest

  context 'bury of reserved job' do

    setup do
      message = uuid
      client.transmit("use #{tube_name}")
      client.transmit("watch #{tube_name}")
      client.transmit("put 0 0 10 #{message.bytesize}\r\n#{message}")
      timeout(2) do
        @reserved_id = client.transmit('reserve')[:id]
      end
    end

    should 'bury a reserved job' do
      response = client.transmit("bury #{@reserved_id} 0")
      assert_equal 'BURIED', response[:status]
      assert_equal 'buried', client.transmit("stats-job #{@reserved_id}")[:body]['state']
    end


    should 'bury a reserved job if the job deadline is approaching' do
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
      response = client.transmit("bury #{@reserved_id} 0")
      assert_equal 'BURIED', response[:status]
      assert_equal 'buried', client.transmit("stats-job #{@reserved_id}")[:body]['state']
    end


    should 'bury a reserved job with the given priority' do
      pri = 1023
      response = client.transmit("bury #{@reserved_id} #{pri}")
      assert_equal 'BURIED', response[:status]
      job_stats = client.transmit("stats-job #{@reserved_id}")[:body]
      assert_equal 'buried', job_stats['state']
      assert_equal pri, job_stats['pri']
    end


    should 'increment buries count for job, current-jobs-buried for tube and server, and server cmd-bury counts' do
      initial_job_buries = client.transmit("stats-job #{@reserved_id}")[:body]['buries']
      initial_tube_jobs_buried = client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-buried']
      server_stats = client.transmit('stats')[:body]
      initial_server_cmd_bury = server_stats['cmd-bury']
      initial_server_jobs_buried = server_stats['current-jobs-buried']

      response = client.transmit("bury #{@reserved_id} 0")
      assert_equal 'BURIED', response[:status]
      assert_equal 'buried', client.transmit("stats-job #{@reserved_id}")[:body]['state']

      assert_equal initial_job_buries + 1, client.transmit("stats-job #{@reserved_id}")[:body]['buries']
      assert_equal initial_tube_jobs_buried + 1, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-buried']
      server_stats = client.transmit('stats')[:body]
      assert_equal initial_server_cmd_bury + 1, server_stats['cmd-bury']
      assert_equal initial_server_jobs_buried + 1, server_stats['current-jobs-buried']
    end


    should 'return NOT_FOUND when burying a job that the client has not reserved' do
      initial_cmd_bury = client.transmit('stats')[:body]['cmd-bury']
      client.transmit("release #{@reserved_id} 0 0")
      assert_raises(Beaneater::NotFoundError, 'Expected bury of unreserved job to raise error') do
        client.transmit("bury #{@reserved_id} 0")
      end
      assert_equal(initial_cmd_bury + 1, client.transmit('stats')[:body]['cmd-bury'], 'Expected cmd-bury to be incremented')
    end


    should 'return NOT_FOUND when burying a job reserved by another client' do
      initial_cmd_bury = client.transmit('stats')[:body]['cmd-bury']
      assert_raises(Beaneater::NotFoundError, 'Expected bury of job reserved by another client to raise error') do
        build_client.transmit("bury #{@reserved_id} 0")
      end
      client.transmit("delete #{@reserved_id}")
      assert_equal(initial_cmd_bury + 1, client.transmit('stats')[:body]['cmd-bury'], 'Expected cmd-bury to be incremented')
    end


    should 'return BAD_FORMAT if job_id or priority are not integers' do
      initial_cmd_bury = client.transmit('stats')[:body]['cmd-bury']
      assert_raises(Beaneater::BadFormatError, 'Expected bury of non-integer job_id to raise error') do
        client.transmit("bury foo 0")
      end

      assert_raises(Beaneater::BadFormatError, 'Expected bury with non-integer priority to raise error') do
        client.transmit("bury #{@reserved_id} foo")
      end
      client.transmit("delete #{@reserved_id}")
      assert_equal(initial_cmd_bury, client.transmit('stats')[:body]['cmd-bury'], 'Expected cmd-bury to remain the same')
    end
  end

  context 'bury not requiring reserved job' do
    should 'return NOT_FOUND when burying a job with an ID <= 0' do
      (-1..0).each do |job_id|
        assert_raises(Beaneater::NotFoundError, "Expected bury of job_id #{job_id} to raise NOT_FOUND error") do
          build_client.transmit("bury #{job_id} 0")
        end
      end
    end


    should 'return BAD_FORMAT if priority is negative' do
      assert_raises(Beaneater::BadFormatError, 'Expected bury with negative timeout to raise error') do
        client.transmit("bury 1 -1")
      end
    end


    should 'return BAD_FORMAT if line includes trailing space' do
      assert_raises(Beaneater::BadFormatError, 'Expected bury with trailing space to raise error') do
        client.transmit("bury 1 1 ")
      end
    end


    should 'return BAD_FORMAT if excess arguments' do
      assert_raises(Beaneater::BadFormatError, 'Expected bury with excess arguments to raise error') do
        client.transmit("bury 1 1 1")
      end
    end

  end

end
