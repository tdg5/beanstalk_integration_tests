require 'test_helper'

class StatsJobTest < BeanstalkIntegrationTest

  context 'stats-job' do

    setup do
      client.transmit("use #{tube_name}")
    end


    should 'return stats for the specified job with all expected keys' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
      response = client.transmit("stats-job #{job_id}")
      assert_equal 'OK', response[:status]
      job_stats = response[:body]
      expected_keys = %w[
        id tube state pri age delay ttr time-left file reserves timeouts
        releases buries kicks
      ]
      assert_equal expected_keys, job_stats.keys
      assert_nil job_stats.values.compact!, 'Expected job stats to have no nil values'
      assert_equal(initial_cmd_stats_job + 1, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be incremented')
    end

    # Some stats are tested elsewhere:
    #
    # :id => See below
    # :tube => PutTest
    # :state => PutTest, BuryTest, ReleaseTest
    # :pri => PutTest, BuryTest, ReleaseTest
    # :age => See below
    # :time-left => PutTest, TouchTest
    # :file => NOT TESTED (except for presence)
    # :reserves => ReserveTest
    # :timeouts => TouchTest
    # :releases => ReleaseTest
    # :buries => BuryTest
    # :kicks => KickTest, KickJobTest
    #
    # For those that aren't...

    should 'return correct id' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      assert_equal(job_id.to_i, client.transmit("stats-job #{job_id}")[:body]['id'], 'Expected job-stats[id] to return correct job id')
    end


    should 'return correct age' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      initial_job_age = client.transmit("stats-job #{job_id}")[:body]['age']
      sleep 1
      assert_equal(initial_job_age + 1, client.transmit("stats-job #{job_id}")[:body]['age'], 'Expected job-stats[age] to return incremented job age')
    end


    should 'return NOT_FOUND if the job has never existed' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
      assert_raises(Beaneater::NotFoundError, 'Expected job-stats for non-existent job to return NOT_FOUND') do
        client.transmit("stats-job #{job_id.to_i + 1}")
      end
      assert_equal(initial_cmd_stats_job + 1, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be incremented')
    end


    should 'return NOT_FOUND if the job is deleted' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      client.transmit("delete #{job_id}")
      initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
      assert_raises(Beaneater::NotFoundError, 'Expected job-stats for deleted job to return NOT_FOUND') do
        client.transmit("stats-job #{job_id}")
      end
      assert_equal(initial_cmd_stats_job + 1, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be incremented')
    end


    should 'return BAD_FORMAT without trailing space' do
      initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
      assert_raises(Beaneater::BadFormatError, 'Expected job-stats without space to return BAD_FORMAT') do
        client.transmit('stats-job')
      end
      assert_equal(initial_cmd_stats_job, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be unchanged')
    end


    should 'return NOT_FOUND if job id is null, negative, or non-integer' do
      ['', -5, 'foo'].each do |job_id|
        initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
        assert_raises(Beaneater::NotFoundError, 'Expected job-stats for invalid job id to return NOT_FOUND') do
          client.transmit("stats-job #{job_id}")
        end
        assert_equal(initial_cmd_stats_job + 1, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be incremented')
      end
    end


    should 'ignore trailing space, arguments, and chars' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      [' ', ' and ignore these arguments', '_and_ignore_these_letters'].each do |trailer|
        initial_cmd_stats_job = client.transmit('stats')[:body]['cmd-stats-job']
        client.transmit("stats-job #{job_id}#{trailer}")
        assert_equal(initial_cmd_stats_job + 1, client.transmit('stats')[:body]['cmd-stats-job'], 'Expected cmd-stats-job to be incremented')
      end
    end

  end

end
