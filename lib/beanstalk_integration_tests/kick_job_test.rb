require 'test_helper'

class KickJobTest < BeanstalkIntegrationTest

  context 'kick-job' do

    setup do
      client.transmit("use #{tube_name}")
      client.transmit("watch #{tube_name}")
    end


    should 'kick the specified buried job' do
      message = uuid
      {
        'buried' => create_buried_jobs(1).first,
        'delayed' => client.transmit("put 0 120 120 #{message.bytesize}\r\n#{message}")[:id],
      }.each_pair do |job_state, job_id|
        assert_equal job_state, client.transmit("stats-job #{job_id}")[:body]['state']
        response = client.transmit("kick-job #{job_id}")
        assert_equal 'KICKED', response[:status]
        job_stats = client.transmit("stats-job #{job_id}")[:body]
        assert_equal('ready', job_stats['state'], "Expected #{job_state} job to be ready after kick")
        assert_equal(1, job_stats['kicks'], 'Expected job kicks to be incremented')
        assert_equal tube_name, job_stats['tube']
      end
    end


    should 'return NOT_FOUND if the job is not in a kickable state' do
      message = uuid
      job_id = client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")[:id]
      response = nil
      assert_raises(Beaneater::NotFoundError, 'Expected kicking unkickable job to return NOT_FOUND') do
        response = client.transmit("kick-job #{job_id}")
      end
      assert_equal(0, client.transmit("stats-job #{job_id}")[:body]['kicks'], 'Expected job kicks to be unchanged')
    end


    should 'return UNKNOWN_COMMAND unless command followed by space' do
      assert_raises(Beaneater::UnknownCommandError, 'Expected kick-job without a space to return UNKNOWN_COMMAND') do
        client.transmit('kick-job')
      end
    end


    should 'return NOT_FOUND when job_id is null, negative, or non-integer' do
      ['', -1, 'x'].each do |job_id|
        assert_raises(Beaneater::NotFoundError, 'Expected kick-job with bad job id to return NOT_FOUND') do
          client.transmit("kick-job #{job_id}")
        end
      end
    end


    should 'ignore trailing space, arguments, and chars' do
      [' ', ' and ignore these arguments', '_and_ignore_these_letters'].each do |trailer|
        job_id = create_buried_jobs(1).first
        client.transmit("kick-job #{job_id}#{trailer}")
        assert_equal 1, client.transmit("stats-job #{job_id}")[:body]['kicks']
        client.transmit("delete #{job_id}")
      end
    end

  end

end
