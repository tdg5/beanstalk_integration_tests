require 'test_helper'

class PutTest < BeanstalkIntegrationTest

  context 'put' do

    setup do
      client.transmit("use #{tube_name}")
      @message = uuid
    end


    should "insert a job into the connection's currently used tube" do
      assert_equal tube_name, client.transmit('list-tube-used')[:id]

      stats = client.transmit('stats')[:body]
      initial_current_producers = stats['current-producers']
      initial_cmd_put = stats['cmd-put']
      response = client.transmit("put 1 0 120 #{@message.bytesize}\r\n#{@message}")
      job_tube = client.transmit("stats-job #{response[:id]}")[:body]['tube']
      stats = client.transmit('stats')[:body]
      assert_equal initial_cmd_put + 1, stats['cmd-put']
      assert_equal initial_current_producers + 1, stats['current-producers']
      assert_equal tube_name, job_tube
    end


    should 'insert a job with the given priority' do
      pri = rand(2**32) - 1
      response = client.transmit("put #{pri} 0 120 #{@message.bytesize}\r\n#{@message}")
      job_pri = client.transmit("stats-job #{response[:id]}")[:body]['pri']

      assert_equal pri, job_pri
    end


    should 'should reduce excessive priorities to their modulus' do
      overture = rand(10)
      pri = 2**32 + overture
      response = client.transmit("put #{pri} 0 120 #{@message.bytesize}\r\n#{@message}")
      job_pri = client.transmit("stats-job #{response[:id]}")[:body]['pri']

      assert_equal overture, job_pri
    end


    should 'return BAD_FORMAT for negative priority, delay, ttr, or size' do
      initial_cmd_put = client.transmit('stats')[:body]['cmd-put']
      args = [-1, 1, 1, 1]
      4.times do |index|
        assert_raises Beaneater::BadFormatError do
          client.transmit("put #{args[0]} #{args[1]} #{args[2]} #{args[3]}")
          assert_equal initial_cmd_put + 1 + index, client.transmit('stats')[:body]['cmd-put']
          args.cycle
        end
      end
    end


    should 'insert a job with the given delay and a state of delayed' do
      delay = 10

      response = client.transmit("put 0 #{delay} 10 #{@message.bytesize}\r\n#{@message}")
      job_stats = client.transmit("stats-job #{response[:id]}")[:body]

      assert_equal delay, job_stats['delay']
      assert_equal 'delayed', job_stats['state']
      sleep 1
      time_left = client.transmit("stats-job #{response[:id]}")[:body]['time-left']
      assert(time_left < job_stats['time-left'], 'Expected time-left to decrement after a second')
    end


    should 'insert a delayed job which becomes ready after the timeout' do
      delay = 1

      response = client.transmit("put 0 #{delay} 10 #{@message.bytesize}\r\n#{@message}")
      sleep 1
      job_stats = client.transmit("stats-job #{response[:id]}")[:body]
      assert_equal 'ready', job_stats['state']

      stats = client.transmit('stats')[:body]
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal(1, stats['current-jobs-urgent'])
      assert_equal(1, stats['current-jobs-ready'])
      assert_equal(1, tube_stats['current-jobs-urgent'])
      assert_equal(1, tube_stats['current-jobs-ready'])
    end


    should 'only update current-jobs-delayed counter on tube and server for delayed job' do
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      tube_initial_jobs_ready = tube_stats['current-jobs-ready'].to_i
      tube_initial_jobs_urgent = tube_stats['current-jobs-urgent'].to_i
      tube_initial_jobs_delayed = tube_stats['current-jobs-delayed'].to_i
      tube_initial_jobs_total = tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      server_initial_jobs_ready = server_stats['current-jobs-ready'].to_i
      server_initial_jobs_urgent = server_stats['current-jobs-urgent'].to_i
      server_initial_jobs_delayed = server_stats['current-jobs-delayed'].to_i
      server_initial_jobs_total = server_stats['total-jobs'].to_i

      client.transmit("put 0 120 10 #{@message.bytesize}\r\n#{@message}")

      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal tube_initial_jobs_ready, tube_stats['current-jobs-ready'].to_i
      assert_equal tube_initial_jobs_urgent, tube_stats['current-jobs-urgent'].to_i
      assert_equal tube_initial_jobs_delayed + 1, tube_stats['current-jobs-delayed'].to_i
      assert_equal tube_initial_jobs_total + 1, tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      assert_equal server_initial_jobs_ready, server_stats['current-jobs-ready'].to_i
      assert_equal server_initial_jobs_urgent, server_stats['current-jobs-urgent'].to_i
      assert_equal server_initial_jobs_delayed + 1, server_stats['current-jobs-delayed'].to_i
      assert_equal server_initial_jobs_total + 1, server_stats['total-jobs'].to_i
    end


    should 'insert a job with the given ttr' do
      ttr = 10

      response = client.transmit("put 0 0 #{ttr} #{@message.bytesize}\r\n#{@message}")
      job_ttr = client.transmit("stats-job #{response[:id]}")[:body]['ttr']

      assert_equal ttr, job_ttr
    end


    should 'increase TTR to 1 if 0 given' do
      response = client.transmit("put 0 0 0 #{@message.bytesize}\r\n#{@message}")
      job_ttr = client.transmit("stats-job #{response[:id]}")[:body]['ttr']

      assert_equal 1, job_ttr
    end


    should 'raise EXPECTED_CRLF if job exceeds declared size but still increment cmd-job' do
      stats = client.transmit('stats')[:body]
      assert_raises Beaneater::ExpectedCrlfError do
        client.transmit("put 0 0 10 #{@message.bytesize - 1}\r\n#{@message}")
      end
      assert_equal stats['cmd-put'] + 1, build_client.transmit('stats')[:body]['cmd-put']
    end


    should 'should be able to handle job of max-job-size' do
      stats = client.transmit('stats')[:body]
      max_job_size = stats['max-job-size']
      dec_mod = max_job_size % 10
      message = SecureRandom.base64(8)[0, 10] * ((max_job_size - dec_mod) / 10)
      message += SecureRandom.base64(dec_mod)[0, dec_mod]

      response = nil
      begin
        response = client.transmit("put 123 0 10 #{message.bytesize}\r\n#{message}")
      rescue Beaneater::JobTooBigError => e
        puts 'Either max-job-size is incorrect or server cannot handle max job size'
        raise e
      end
      assert_equal 'INSERTED', response[:status]
      assert_equal stats['cmd-put'] + 1, client.transmit('stats')[:body]['cmd-put']
      assert_equal message, client.transmit("peek #{response[:id]}")[:body]
    end


    should 'raise JOB_TOO_BIG if job larger than max job size but still increment cmd-job' do
      stats = client.transmit('stats')[:body]
      max_job_size = stats['max-job-size']
      dec_mod = max_job_size % 10
      message = SecureRandom.base64(8)[0, 10] * ((max_job_size - dec_mod) / 10)
      message += SecureRandom.base64(dec_mod)[0, dec_mod + 1]

      assert_raises Beaneater::JobTooBigError do
        client.transmit("put 123 0 10 #{message.bytesize}\r\n#{message}")
      end
      assert_equal stats['cmd-put'] + 1, client.transmit('stats')[:body]['cmd-put']
    end


    should 'insert a job with the given body' do
      response = client.transmit("put 0 0 10 #{@message.bytesize}\r\n#{@message}")
      assert_equal @message, client.transmit("peek #{response[:id]}")[:body]
    end


    should 'handle any byte value and incur no length weirdness' do
      bytes = (0..255).to_a.pack("c*")
      response = client.transmit("put 0 0 100 256\r\n#{bytes}")
      assert_equal bytes, client.transmit("peek #{response[:id]}")[:body]
    end


    should 'pass non-terminal \r\n through without modification or length weirdness' do
      message = "\r\n" * 10
      response = client.transmit("put 0 0 100 20\r\n#{message}")
      assert_equal message, client.transmit("peek #{response[:id]}")[:body]
    end


    should 'increment current-jobs-ready and current-jobs-urgent counters on server and tube if priority < 1024' do
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      tube_initial_jobs_ready = tube_stats['current-jobs-ready'].to_i
      tube_initial_jobs_urgent = tube_stats['current-jobs-urgent'].to_i
      tube_initial_jobs_total = tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      server_initial_jobs_ready = server_stats['current-jobs-ready'].to_i
      server_initial_jobs_urgent = server_stats['current-jobs-urgent'].to_i
      server_initial_jobs_total = server_stats['total-jobs'].to_i

      client.transmit("put 1023 0 100 #{@message.bytesize}\r\n#{@message}")

      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal tube_initial_jobs_ready + 1, tube_stats['current-jobs-ready'].to_i
      assert_equal tube_initial_jobs_urgent + 1, tube_stats['current-jobs-urgent'].to_i
      assert_equal tube_initial_jobs_total + 1, tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      assert_equal server_initial_jobs_ready + 1, server_stats['current-jobs-ready'].to_i
      assert_equal server_initial_jobs_urgent + 1, server_stats['current-jobs-urgent'].to_i
      assert_equal server_initial_jobs_total + 1, server_stats['total-jobs'].to_i
    end


    should 'not update current-jobs-urgent counters on server and tube if priority >= 1024' do
      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      tube_initial_jobs_ready = tube_stats['current-jobs-ready'].to_i
      tube_initial_jobs_urgent = tube_stats['current-jobs-urgent'].to_i
      tube_initial_jobs_total = tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      server_initial_jobs_ready = server_stats['current-jobs-ready'].to_i
      server_initial_jobs_urgent = server_stats['current-jobs-urgent'].to_i
      server_initial_jobs_total = server_stats['total-jobs'].to_i

      client.transmit("put 1024 0 100 #{@message.bytesize}\r\n#{@message}")

      tube_stats = client.transmit("stats-tube #{tube_name}")[:body]
      assert_equal tube_initial_jobs_ready + 1, tube_stats['current-jobs-ready'].to_i
      assert_equal tube_initial_jobs_urgent, tube_stats['current-jobs-urgent'].to_i
      assert_equal tube_initial_jobs_total + 1, tube_stats['total-jobs'].to_i
      server_stats = client.transmit('stats')[:body]
      assert_equal server_initial_jobs_ready + 1, server_stats['current-jobs-ready'].to_i
      assert_equal server_initial_jobs_urgent, server_stats['current-jobs-urgent'].to_i
      assert_equal server_initial_jobs_total + 1, server_stats['total-jobs'].to_i
    end


    should 'raise BAD_FORMAT with trailing_space but still increment cmd-put' do
      initial_cmd_put = client.transmit('stats')[:body]['cmd-put']
      assert_raises(Beaneater::BadFormatError, 'Expected put with trailing space to return BAD_FORMAT') do
        client.transmit("put 0 0 120 2 \r\nxx")
      end
      assert_equal initial_cmd_put + 1, build_client.transmit('stats')[:body]['cmd-put']
    end

  end

end
