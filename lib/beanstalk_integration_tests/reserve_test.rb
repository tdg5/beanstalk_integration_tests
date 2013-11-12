require 'beanstalk_integration_tests/test_helper'
require 'timeout'

class ReserveTest < BeanstalkIntegrationTest

  context 'common reserve behavior' do

    setup do
      client.transmit("watch #{tube_name}")
      client.transmit("use #{tube_name}")
      @message = uuid
    end

    {
      'reserve' => 'reserve',
      'reserve-with-timeout' => 'reserve-with-timeout 0'
    }.each do |command, reserve_command|
      context reserve_command.split(/\s/).first do

        context 'a single watched tube' do

          should 'reserve a job' do
            put_response = client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")
            job_id = put_response[:id]
            assert_equal tube_name, client.transmit("stats-job #{job_id}")[:body]['tube']
            assert client.transmit('list-tubes-watched')[:body].include?(tube_name)
            timeout(2) do
              reservation_id = client.transmit(reserve_command)[:id]
              assert_equal job_id, reservation_id
              client.transmit("delete #{job_id}")
            end
          end


          should "increment reserves count on job, current-jobs-reserved on tube, and current-jobs-reserved, current-producers, and cmd-#{command} on server" do
            other_client = build_client
            other_client.transmit("use #{tube_name}")
            job_id = other_client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            initial_job_reserves = client.transmit("stats-job #{job_id}")[:body]['reserves']
            initial_tube_reserved = client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-reserved']
            server_stats = client.transmit('stats')[:body]
            initial_server_reserved = server_stats['current-jobs-reserved']
            initial_server_cmds = server_stats["cmd-#{command}"]
            initial_server_workers = server_stats['current-workers']

            timeout(2) do
              client.transmit(reserve_command)[:id]
            end

            assert_equal initial_job_reserves + 1, client.transmit("stats-job #{job_id}")[:body]['reserves']
            assert_equal initial_tube_reserved + 1, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-reserved']
            server_stats = client.transmit('stats')[:body]
            assert_equal initial_server_reserved + 1, server_stats['current-jobs-reserved']
            assert_equal initial_server_cmds + 1, server_stats["cmd-#{command}"]
            assert_equal initial_server_workers + 1, server_stats['current-workers']
            client.transmit("delete #{job_id}")
          end


          should 'receive a job enqueued to the watched tube while reserving' do
            client2 = build_client
            client2.transmit("use #{tube_name}")

            thread = Thread.new do
              timeout(2) do
                alt_command = command == 'reserve' ? 'reserve' : 'reserve-with-timeout 1'
                reservation_id = client.transmit(alt_command)[:id]
                assert reservation_id
                client.transmit("delete #{reservation_id}")
              end
              assert_equal 0, client.transmit("stats-tube #{tube_name}")[:body]['current-waiting']
            end
            while client2.transmit("stats-tube #{tube_name}")[:body]['current-waiting'] == 0
              sleep 0.1
            end
            client2.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")
            thread.join
          end


          should 'reserve the job with the lowest priority' do
            job_ids = []
            job_ids << client.transmit("put 5000 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids << client.transmit("put 1024 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids << client.transmit("put 1023 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids << client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids.length.times do
              timeout(2) do
                job_id = job_ids.pop
                assert_equal job_id, client.transmit(reserve_command)[:id]
                client.transmit("delete #{job_id}")
              end
            end
          end


          should 'reserve the job received first if priorities equal' do
            job_ids = 4.times.map do
              client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            end
            job_ids.length.times do
              timeout(2) do
                job_id = job_ids.shift
                assert_equal job_id, client.transmit(reserve_command)[:id]
                client.transmit("delete #{job_id}")
              end
            end
          end

        end

        context 'multiple watched tubes' do

          setup do
            @client2 = build_client
            @client2_tube = generate_tube_name
            @client2.transmit("use #{@client2_tube}")
            @client3 = build_client
            @client3_tube = generate_tube_name
            @client3.transmit("use #{@client3_tube}")
            client.transmit("watch #{@client2_tube}")
            client.transmit("watch #{@client3_tube}")
          end


          should 'reserve a job from any watched tube' do
            [client, @client2, @client3].each do |tube_client|
              timeout(2) do
                job_id = tube_client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
                assert_equal job_id, client.transmit(reserve_command)[:id]
                client.transmit("delete #{job_id}")
              end
            end
          end


          should 'receive a job enqueued to any watched tube while reserving' do
            client_clone = build_client
            client_clone.transmit("use #{tube_name}")
            alt_command = command == 'reserve' ? 'reserve' : 'reserve-with-timeout 1'
            {client_clone => tube_name, @client2 => @client2_tube, @client3 => @client3_tube}.each do |tube_client, tube_client_tube|
              thread = Thread.new do
                timeout(2) do
                  job_id = client.transmit(alt_command)[:id]
                  assert_equal tube_client_tube, client.transmit("stats-job #{job_id}")[:body]['tube']
                  client.transmit("delete #{job_id}")
                end
              end

              while tube_client.transmit("stats-tube #{tube_client_tube}")[:body]['current-waiting'] == 0
                sleep 0.1
              end

              tube_client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")
              thread.join
            end
          end


          should 'reserve the job with the lowest priority' do
            job_ids = []
            subset_ids = []
            [5000, 1024, 1023, 0].each do |pri|
              subset_ids << client.transmit("put #{pri} 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
              subset_ids << @client2.transmit("put #{pri} 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
              subset_ids << @client3.transmit("put #{pri} 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
              job_ids.concat(subset_ids.reverse)
              subset_ids.clear
            end
            job_ids.length.times do
              timeout(2) do
                job_id = job_ids.pop
                assert_equal job_id, client.transmit(reserve_command)[:id]
                client.transmit("delete #{job_id}")
              end
            end
          end


          should 'reserve the job received first if priorities equal' do
            job_ids = []
            job_ids << client.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids << @client2.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]
            job_ids << @client3.transmit("put 0 0 60 #{@message.bytesize}\r\n#{@message}")[:id]

            client.transmit("watch #{@client2_tube}")
            client.transmit("watch #{@client3_tube}")

            job_ids.length.times do
              timeout(2) do
                job_id = job_ids.shift
                assert_equal job_id, client.transmit(reserve_command)[:id]
                client.transmit("delete #{job_id}")
              end
            end
          end


          should 'not reserve a job from a tube not watched' do
            client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")
            client.transmit("watch #{generate_tube_name}")
            client.transmit("ignore #{tube_name}")

            expected_error = command == 'reserve' ? Timeout::Error : Beaneater::TimedOutError
            assert_raises(expected_error, 'Expected client to timeout while waiting for reservation') do
              timeout(1) do
                client.transmit(reserve_command)
              end
            end
          end


          should 'supply clients with jobs in the order client reservations were received' do
            clients = Hash[[build_client, @client2, @client3].shuffle.zip([0, 1, 2])]
            alt_command = command == 'reserve' ? 'reserve' : 'reserve-with-timeout 1'
            threads = []
            clients.each_pair do |tube_client, reservation_number|
              tube_client.transmit("watch #{tube_name}")
              sleep 0.25
              threads << Thread.new do
                timeout(3) do
                  reservation = tube_client.transmit(alt_command)
                  assert_equal reservation_number.to_s, reservation[:body][-1]
                  tube_client.transmit("delete #{reservation[:id]}")
                end
              end
            end

            while client.transmit("stats-tube #{tube_name}")[:body]['current-waiting'] != 3
              sleep 0.1
            end

            3.times do |reservation_number|
              message = [@message, reservation_number].join(' ')
              client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")
            end
            threads.each(&:join)
          end


          should 'return a reserved job to the ready state and correct tube if the ttr expires' do
            [client, @client2, @client3].each do |tube_client|
              tube_client.transmit("put 0 0 1 #{@message.bytesize}\r\n#{@message}")
            end
            [tube_name, @client2_tube, @client3_tube].each do |tube_client_tube|
              job_id = nil
              timeout(2) do
                job_id = client.transmit('reserve')[:id]
              end
              assert_equal 'reserved', client.transmit("stats-job #{job_id}")[:body]['state']
              sleep 1
              job_stats = client.transmit("stats-job #{job_id}")[:body]
              assert_equal 'ready', job_stats['state']
              assert_equal tube_client_tube, job_stats['tube']
              client.transmit("delete #{job_id}")
            end
          end

        end


        should 'return deadline_soon if a ttr expiration is pending' do
          job_id = client.transmit("put 0 0 1 #{@message.bytesize}\r\n#{@message}")[:id]
          timeout(2) do
            client.transmit('reserve')
            assert_raises(Beaneater::DeadlineSoonError, 'Expected reserve during ttr expiration to return deadlinesoon') do
              client.transmit('reserve')
            end
          end
          client.transmit("delete #{job_id}")
        end

      end

    end

  end


  context 'reserve' do

    should 'return BAD_FORMAT with trailing space or any trailing args' do
      initial_cmd_reserve = client.transmit('stats')[:body]['cmd-reserve']
      timeout(1) do
        assert_raises(Beaneater::BadFormatError, 'Expected reserve with trailing space to return BAD_FORMAT') do
          client.transmit('reserve ')
        end
      end
      assert_equal(initial_cmd_reserve, client.transmit('stats')[:body]['cmd-reserve'], 'Expected cmd-reserve to be unchanged')

      timeout(1) do
        assert_raises(Beaneater::BadFormatError, 'Expected reserve with args to return BAD_FORMAT') do
          client.transmit('reserve 1')
        end
      end
      assert_equal(initial_cmd_reserve, client.transmit('stats')[:body]['cmd-reserve'], 'Expected cmd-reserve to be unchanged')
    end

  end

  context 'reserve-with-timeout' do

    should 'return TIMED_OUT immediately if timeout of zero and no jobs available' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      timeout(1) do
        assert_raises(Beaneater::TimedOutError, 'Expected reserve-with-timeout to timeout') do
          client.transmit('reserve-with-timeout 0')
        end
      end
      assert_equal(initial_cmd_reserve_with_timeout + 1, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be incremented')
    end


    should 'return TIMED_OUT after timeout if no jobs available' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      timeout(2) do
        assert_raises(Beaneater::TimedOutError, 'Expected reserve-with-timeout to timeout') do
          client.transmit('reserve-with-timeout 1')
        end
      end
      assert_equal(initial_cmd_reserve_with_timeout + 1, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be incremented')
    end


    should 'return BAD_FORMAT without space trailing space or timeout' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      timeout(2) do
        assert_raises(Beaneater::BadFormatError, 'Expected reserve-with-timeout to return BAD_FORMAT without trailing space or timeout') do
          client.transmit('reserve-with-timeout')
        end
      end
      assert_equal(initial_cmd_reserve_with_timeout, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be unchanged')
    end


    should 'should coerce non-integer timeouts to zero' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      message = uuid
      job_id = client.transmit("put 0 0 1 #{message.bytesize}\r\n#{message}")[:id]
      timeout(1) do
        assert_equal job_id, client.transmit("reserve-with-timeout I really don't think this should be valid.")[:id]
      end
      client.transmit("delete #{job_id}")
      assert_equal(initial_cmd_reserve_with_timeout + 1, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be incremented')
    end


    should 'should coerce space with no timeout to zero' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      message = uuid
      job_id = client.transmit("put 0 0 1 #{message.bytesize}\r\n#{message}")[:id]
      timeout(1) do
        assert_equal job_id, client.transmit("reserve-with-timeout ")[:id]
      end
      client.transmit("delete #{job_id}")
      assert_equal(initial_cmd_reserve_with_timeout + 1, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be incremented')
    end


    should 'should handle negative timeout like normal reserve' do
      initial_cmd_reserve_with_timeout = client.transmit('stats')[:body]['cmd-reserve-with-timeout']
      message = uuid
      client.transmit("watch #{tube_name}")
      other_client = build_client
      other_client.transmit("use #{tube_name}")

      reserve_thread = Thread.new do
        value = nil
        timeout(2) do
          value = client.transmit("reserve-with-timeout -1")[:id]
        end
        value
      end

      # Sleep one to verify abs(timeout) not used
      sleep 1
      job_id = other_client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      assert_equal job_id, reserve_thread.value
      assert_equal(initial_cmd_reserve_with_timeout + 1, client.transmit('stats')[:body]['cmd-reserve-with-timeout'], 'Expected cmd-reserve-with-timeout to be incremented')
      client.transmit("delete #{job_id}")
    end

  end

end
