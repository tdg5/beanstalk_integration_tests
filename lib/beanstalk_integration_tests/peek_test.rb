require 'beanstalk_integration_tests/test_helper'

class PeekTest < BeanstalkIntegrationTest

  setup do
    client.transmit("use #{tube_name}")
    client.transmit("watch #{tube_name}")
  end


  context 'peek' do

    should 'return job id and message' do
      job_id, message = do_setup
      response = client.transmit("peek #{job_id}")
      assert_equal('FOUND', response[:status], 'Expected peek of existing job to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek of existing job to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek of existing job to return the correct body')
    end


    should 'return NOT_FOUND if null, non-integer, or negative job id provided or no such job' do
      relative_id = do_setup.first
      ['', -1, 'foo', relative_id.to_i + 1].each do |job_id|
        initial_cmd_peek = client.transmit('stats')[:body]['cmd-peek']
        assert_raises(Beaneater::NotFoundError, 'Expected peeking non-existent job to return NOT_FOUND') do
          response = client.transmit("peek #{job_id}")
        end
        assert_equal(initial_cmd_peek + 1, client.transmit('stats')[:body]['cmd-peek'], 'Expected cmd-peek to be incremented')
      end
    end


    should 'return UNKNOWN_COMMAND if command does not include space' do
      initial_cmd_peek = client.transmit('stats')[:body]['cmd-peek']
      assert_raises(Beaneater::UnknownCommandError, 'Expected peek without space to return UNKNOWN_COMMAND') do
        client.transmit('peek')
      end
      assert_equal(initial_cmd_peek, client.transmit('stats')[:body]['cmd-peek'], 'Expected cmd-peek to be unchanged')
    end


    should 'peek ignoring trailing space, args, and chars' do
      job_id, message = do_setup
      [' ', ' and ignore these arguments', '_and_ignore_these_letters'].each do |trailer|
        initial_cmd_peek = client.transmit('stats')[:body]['cmd-peek']
        response = client.transmit("peek #{job_id}#{trailer}")
        assert_equal job_id, response[:id]
        assert_equal message, response[:body]
        assert_equal(initial_cmd_peek + 1, client.transmit('stats')[:body]['cmd-peek'], 'Expected cmd-peek to be unchanged')
      end
    end

  end


  context 'peek-ready' do

    should 'return job id and message' do
      job_id, message = do_setup
      initial_cmd_peek_ready = client.transmit('stats')[:body]['cmd-peek-ready']
      response = client.transmit('peek-ready')
      assert_equal('FOUND', response[:status], 'Expected peek-ready to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-ready to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-ready to return the correct body')
      assert_equal(initial_cmd_peek_ready + 1, client.transmit('stats')[:body]['cmd-peek-ready'], 'Expected cmd-peek-ready to be incremented')
    end


    should 'return the correct ready job' do
      initial_cmd_peek_ready = client.transmit('stats')[:body]['cmd-peek-ready']
      buried_id = do_setup.first
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{buried_id} 0")
      do_setup(10)
      do_setup(0, 120)
      job_id, message = do_setup
      response = client.transmit('peek-ready')
      assert_equal('FOUND', response[:status], 'Expected peek-ready to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-ready to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-ready to return the correct body')
      assert_equal(initial_cmd_peek_ready + 1, client.transmit('stats')[:body]['cmd-peek-ready'], 'Expected cmd-peek-ready to be incremented')
    end


    should 'return NOT_FOUND if no ready jobs' do
      buried_id = do_setup.first
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{buried_id} 0")
      do_setup(0, 120)
      assert_raises(Beaneater::NotFoundError, 'Expected peek-ready with no ready jobs to return NOT_FOUND') do
        client.transmit('peek-ready')
      end
    end


    should 'return BAD_FORMAT with any trailing characters' do
      initial_cmd_peek_ready = client.transmit('stats')[:body]['cmd-peek-ready']
      assert_raises(Beaneater::BadFormatError, 'Expected peek-ready with trailing characters to return BAD_FORMAT') do
        [' ', ' and other args'].each do |trailer|
          client.transmit("peek-ready#{trailer}")
        end
      end
      assert_equal(initial_cmd_peek_ready, client.transmit('stats')[:body]['cmd-peek-ready'], 'Expected cmd-peek-ready to be unchanged')
    end

  end


  context 'peek-delayed' do

    should 'return job id and message' do
      job_id, message = do_setup(0, 120)
      assert_equal('delayed', client.transmit("stats-job #{job_id}")[:body]['state'], 'Expected job enqueued with delay to be delayed')
      initial_cmd_peek_delayed = client.transmit('stats')[:body]['cmd-peek-delayed']
      response = client.transmit('peek-delayed')
      assert_equal('FOUND', response[:status], 'Expected peek-delayed of existing job to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-delayed to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-delayed to return the correct body')
      assert_equal(initial_cmd_peek_delayed + 1, client.transmit('stats')[:body]['cmd-peek-delayed'], 'Expected cmd-peek-delayed to be incremented')
    end


    should 'return the correct delayed job' do
      initial_cmd_peek_delayed = client.transmit('stats')[:body]['cmd-peek-delayed']
      buried_id = do_setup.first
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{buried_id} 0")
      do_setup(10)
      do_setup(0, 120)
      do_setup(0, 60)
      job_id, message = do_setup(10, 30)
      response = client.transmit('peek-delayed')
      assert_equal('FOUND', response[:status], 'Expected peek-delayed to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-delayed to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-delayed to return the correct body')
      assert_equal(initial_cmd_peek_delayed + 1, client.transmit('stats')[:body]['cmd-peek-delayed'], 'Expected cmd-peek-delayed to be incremented')
    end


    should 'return NOT_FOUND if no delayed jobs' do
      buried_id = do_setup.first
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{buried_id} 0")
      do_setup
      assert_raises(Beaneater::NotFoundError, 'Expected peek-delayed with no delayed jobs to return NOT_FOUND') do
        client.transmit('peek-delayed')
      end
    end


    should 'return BAD_FORMAT with any trailing characters' do
      initial_cmd_peek_delayed = client.transmit('stats')[:body]['cmd-peek-delayed']
      assert_raises(Beaneater::BadFormatError, 'Expected peek-delayed with trailing characters to return BAD_FORMAT') do
        [' ', ' and other args'].each do |trailer|
          client.transmit("peek-delayed#{trailer}")
        end
      end
      assert_equal(initial_cmd_peek_delayed, client.transmit('stats')[:body]['cmd-peek-delayed'], 'Expected cmd-peek-delayed to be unchanged')
    end

  end


  context 'peek-buried' do

    should 'return job id and message' do
      job_id, message = do_setup
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{job_id} 0")
      assert_equal('buried', client.transmit("stats-job #{job_id}")[:body]['state'], 'Expected job enqueued with delay to be delayed')
      initial_cmd_peek_buried = client.transmit('stats')[:body]['cmd-peek-buried']
      response = client.transmit('peek-buried')
      assert_equal('FOUND', response[:status], 'Expected peek-buried of existing job to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-buried to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-buried to return the correct body')
      assert_equal(initial_cmd_peek_buried + 1, client.transmit('stats')[:body]['cmd-peek-buried'], 'Expected cmd-peek-buried to be incremented')
    end


    should 'return the correct buried job' do
      initial_cmd_peek_buried = client.transmit('stats')[:body]['cmd-peek-buried']
      do_setup(10)
      do_setup(0, 120)
      job_id, message = do_setup
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{job_id} 0")
      buried_id = do_setup.first
      timeout(1) do
        client.transmit('reserve')
      end
      client.transmit("bury #{buried_id} 0")
      response = client.transmit('peek-buried')
      assert_equal('FOUND', response[:status], 'Expected peek-buried to return FOUND')
      assert_equal(job_id, response[:id], 'Expected peek-buried to return the correct job id')
      assert_equal(message, response[:body], 'Expected peek-buried to return the correct body')
      assert_equal(initial_cmd_peek_buried + 1, client.transmit('stats')[:body]['cmd-peek-buried'], 'Expected cmd-peek-buried to be incremented')
    end


    should 'return NOT_FOUND if no buried jobs' do
      do_setup(0, 120)
      do_setup
      assert_raises(Beaneater::NotFoundError, 'Expected peek-buried with no buried jobs to return NOT_FOUND') do
        client.transmit('peek-buried')
      end
    end


    should 'return BAD_FORMAT with any trailing characters' do
      initial_cmd_peek_buried = client.transmit('stats')[:body]['cmd-peek-buried']
      assert_raises(Beaneater::BadFormatError, 'Expected peek-buried with trailing characters to return BAD_FORMAT') do
        [' ', ' and other args'].each do |trailer|
          client.transmit("peek-buried#{trailer}")
        end
      end
      assert_equal(initial_cmd_peek_buried, client.transmit('stats')[:body]['cmd-peek-buried'], 'Expected cmd-peek-buried to be unchanged')
    end

  end


  def do_setup(pri = 0, delay = 0, ttr = 120, message = uuid)
    job_id = client.transmit("put #{pri} #{delay} #{ttr} #{message.bytesize}\r\n#{message}")[:id]
    return [job_id, message]
  end

end
