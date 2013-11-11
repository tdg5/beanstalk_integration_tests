require 'test_helper'

class KickTest < BeanstalkIntegrationTest

  context 'kick' do

    setup do
      client.transmit("watch #{tube_name}")
      client.transmit("use #{tube_name}")
    end


    should 'return UNKNOWN_COMMAND without a space following the command' do
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      assert_raises(Beaneater::UnknownCommandError, 'Expected kick without a space to return UNKNOWN_COMMAND') do
        client.transmit('kick')
      end
      assert_equal(initial_cmd_kick, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be unchanged')
    end


    should 'return BAD_FORMAT with a null or non-integer bound' do
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      ['', 'foo'].each do |bound|
        assert_raises(Beaneater::BadFormatError, 'Expected kick with an invalid bound to return BAD_FORMAT') do
          client.transmit("kick #{bound}")
        end
      end
      assert_equal(initial_cmd_kick, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be unchanged')
    end


    should 'kick 0 jobs if bound <= 0' do
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      [0, -5].each_with_index do |bound, index|
        response = client.transmit("kick #{bound}")
        assert_equal 'KICKED', response[:status]
        assert_equal '0', response[:id]
        assert_equal(initial_cmd_kick + index + 1, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
      end
    end


    should 'kick 0 jobs if no jobs to kick' do
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      response = client.transmit('kick 1000')
      assert_equal 'KICKED', response[:status]
      assert_equal '0', response[:id]
      assert_equal(initial_cmd_kick + 1, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
    end


    should 'only kick jobs in the currently used tube' do
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      create_buried_jobs
      5.times do
        message = uuid
        client.transmit("put 0 120 120 #{message.bytesize}\r\n#{message}")
      end
      other_client = build_client
      response = other_client.transmit('kick 1000')
      assert_equal 'KICKED', response[:status]
      assert_equal '0', response[:id]
      assert_equal(initial_cmd_kick + 1, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
      assert_equal 0, client.transmit('stats')[:body]['current-jobs-ready']
      other_client.transmit("use #{tube_name}")
      other_client.transmit('kick 5')
      other_client.transmit('kick 5')
      assert_equal 10, other_client.transmit('stats')[:body]['current-jobs-ready']
    end


    should 'ignore trailing space, arguments, and chars' do
      create_buried_jobs
      initial_cmd_kick = client.transmit('stats')[:body]['cmd-kick']
      [' ', ' and ignore these arguments', '_and_ignore_these_letters'].each do |trailer|
        response = client.transmit("kick 1#{trailer}")
        assert_equal 'KICKED', response[:status]
        assert_equal '1', response[:id]
        job_id = client.transmit('peek-ready')[:id]
        assert_equal 1, client.transmit("stats-job #{job_id}")[:body]['kicks']
        client.transmit("delete #{job_id}")
      end
      assert_equal(initial_cmd_kick + 3, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
    end


    context 'buried jobs' do
      should 'kick at most the requested number of jobs' do
        buried_count = 9
        job_ids = create_buried_jobs(buried_count)
        stats = client.transmit('stats')[:body]
        assert_equal buried_count, stats['current-jobs-buried']
        assert_equal buried_count, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-buried']
        initial_cmd_kick = stats['cmd-kick']
        kicked = 0
        [1, 2, 3].each do |slice_size|
          client.transmit("kick #{slice_size}")
          kicked += slice_size
          stats = client.transmit('stats')[:body]
          assert_equal buried_count - kicked, stats['current-jobs-buried']
          assert_equal slice_size, stats['current-jobs-ready']
          slice_size.times do
            job_id = client.transmit('peek-ready')[:id]
            assert_equal job_ids.shift, job_id
            assert_equal(1, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be incremented')
            client.transmit("delete #{job_id}")
          end
        end
        # Kick 4 with only 3 remaining
        client.transmit("kick 4")
        assert_equal 0, client.transmit('stats')[:body]['current-jobs-buried']
        assert_equal 3, client.transmit('stats')[:body]['current-jobs-ready']
        assert_equal 0, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-buried']
        3.times do
          job_id = client.transmit('peek-ready')[:id]
          assert_equal job_ids.shift, job_id
          assert_equal(1, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be incremented')
          client.transmit("delete #{job_id}")
        end

        assert_equal(initial_cmd_kick + 4, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
      end


      should 'kick only buried jobs if buried and delayed jobs available' do
        buried_count = delayed_count = 3
        buried_job_ids = create_buried_jobs(buried_count)
        delayed_job_ids = []
        delayed_count.times do
          message = uuid
          delayed_job_ids << client.transmit("put 0 120 120 #{message.bytesize}\r\n#{message}")[:id]
        end
        stats = client.transmit('stats')[:body]
        assert_equal buried_count, stats['current-jobs-buried']
        assert_equal delayed_count, stats['current-jobs-delayed']
        initial_cmd_kick = stats['cmd-kick']

        client.transmit("kick #{buried_count + delayed_count}")
        stats = client.transmit('stats')[:body]
        assert_equal 0, stats['current-jobs-buried']
        assert_equal buried_count, stats['current-jobs-ready']
        assert_equal delayed_count, stats['current-jobs-delayed']
        buried_count.times do
          job_id = client.transmit('peek-ready')[:id]
          assert_equal buried_job_ids.shift, job_id
          assert_equal(1, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be incremented')
          client.transmit("delete #{job_id}")
        end
        delayed_count.times do
          job_id = client.transmit('peek-delayed')[:id]
          assert_equal delayed_job_ids.shift, job_id
          assert_equal(0, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be unchanged')
          client.transmit("delete #{job_id}")
        end

        assert_equal(initial_cmd_kick + 1, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
      end

    end


    context 'delayed jobs' do
      should 'kick at most the requested number of jobs' do
        delayed_count = 9
        job_ids = delayed_count.times.map do
          message = uuid
          client.transmit("put 0 120 120 #{message.bytesize}\r\n#{message}")[:id]
        end
        stats = client.transmit('stats')[:body]
        assert_equal 0, stats['current-jobs-buried']
        assert_equal delayed_count, stats['current-jobs-delayed']
        assert_equal delayed_count, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-delayed']
        initial_cmd_kick = stats['cmd-kick']
        kicked = 0
        [1, 2, 3].each do |slice_size|
          client.transmit("kick #{slice_size}")
          kicked += slice_size
          stats = client.transmit('stats')[:body]
          assert_equal delayed_count - kicked, stats['current-jobs-delayed']
          assert_equal slice_size, stats['current-jobs-ready']
          slice_size.times do
            job_id = client.transmit('peek-ready')[:id]
            assert_equal job_ids.shift, job_id
            assert_equal(1, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be incremented')
            client.transmit("delete #{job_id}")
          end
        end
        # Kick 4 with only 3 remaining
        client.transmit("kick 4")
        assert_equal 0, client.transmit('stats')[:body]['current-jobs-delayed']
        assert_equal 3, client.transmit('stats')[:body]['current-jobs-ready']
        assert_equal 0, client.transmit("stats-tube #{tube_name}")[:body]['current-jobs-delayed']
        3.times do
          job_id = client.transmit('peek-ready')[:id]
          assert_equal job_ids.shift, job_id
          assert_equal(1, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be incremented')
          client.transmit("delete #{job_id}")
        end

        assert_equal(initial_cmd_kick + 4, client.transmit('stats')[:body]['cmd-kick'], 'Expected cmd-kick to be incremented')
      end
    end

  end

end
