require 'beanstalk_integration_tests/test_helper'

class DeleteTest < BeanstalkIntegrationTest

  setup do
    client.transmit("use #{tube_name}")
    client.transmit("watch #{tube_name}")
    @message = uuid
  end


  context 'delete' do

    should 'delete a job that is ready' do
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      response = client.transmit("delete #{job_id}")
      assert_equal 'DELETED', response[:status]
      assert_equal(initial_server_cmd_delete + 1, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')

      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("peek #{job_id}")
      end
    end


    should 'delete a job that is buried' do
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      timeout(2) do
        client.transmit('reserve')
      end
      bury_response = client.transmit("bury #{job_id} 120")
      assert_equal 'BURIED', bury_response[:status]
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id}")
      assert_equal 'DELETED', response[:status]
      assert_equal(initial_server_cmd_delete + 1, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')

      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("peek #{job_id}")
      end
    end


    should 'delete a job that is delayed' do
      job_id = client.transmit("put 0 120 120 #{@message.bytesize}\r\n#{@message}")[:id]
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id}")
      assert_equal 'DELETED', response[:status]
      assert_equal(initial_server_cmd_delete + 1, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')

      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("peek #{job_id}")
      end
    end


    should 'delete a job that is reserved by the client' do
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      timeout(2) do
        client.transmit('reserve')
      end
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id}")
      assert_equal 'DELETED', response[:status]
      assert_equal(initial_server_cmd_delete + 1, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')
      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("peek #{job_id}")
      end
    end


    should 'not delete a job that is deleted' do
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id}")
      assert_equal 'DELETED', response[:status]
      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("delete #{job_id}")
      end
      assert_equal(initial_server_cmd_delete + 2, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')
    end


    should 'not delete a job that is reserved by another client' do
      client2 = build_client
      client2.transmit("watch #{tube_name}")
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      timeout(2) do
        client2.transmit('reserve')
      end
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      assert_raises(Beaneater::NotFoundError, 'Expected deleted job to not be found') do
        client.transmit("delete #{job_id}")
      end
      client2.transmit("delete #{job_id}")
      assert_equal(initial_server_cmd_delete + 2, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')
    end


    should 'return UNKNOWN_COMMAND without a space and job_id' do
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      assert_raises(Beaneater::UnknownCommandError, 'Expected delete with out space and job_id to raise UNKNOWN_COMMAND') do
        client.transmit('delete')
      end
      assert_equal(initial_server_cmd_delete, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be unchanged')
    end


    should 'return NOT_FOUND with a space but without a job_id' do
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      assert_raises(Beaneater::NotFoundError, 'Expected delete with space but without job_id to raise NOT_FOUND') do
        client.transmit('delete ')
      end
      assert_equal(initial_server_cmd_delete + 1, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
    end


    should 'ignore extraneous parameters' do
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id} but don't care about these params")
      assert_equal 'DELETED', response[:status]
      assert_raises(Beaneater::NotFoundError, 'Expected delete with extraneous parameters to raise NOT_FOUND for deleted job') do
        client.transmit("delete #{job_id} but ignore these params")
      end
      assert_equal(initial_server_cmd_delete + 2, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')
    end


    should 'ignore characters following a valid number' do
      job_id = client.transmit("put 0 0 120 #{@message.bytesize}\r\n#{@message}")[:id]
      initial_server_cmd_delete = client.transmit('stats')[:body]['cmd-delete']
      initial_tube_cmd_delete = client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete']
      response = client.transmit("delete #{job_id}_but_ignore_these_words")
      assert_equal 'DELETED', response[:status]
      assert_raises(Beaneater::NotFoundError, 'Expected delete with extraneous parameters to raise NOT_FOUND for deleted job') do
        client.transmit("delete #{job_id}_but_ignore_these_words")
      end
      assert_equal(initial_server_cmd_delete + 2, client.transmit('stats')[:body]['cmd-delete'], 'Expected server cmd-delete to be incremented')
      assert_equal(initial_tube_cmd_delete + 1, client.transmit("stats-tube #{tube_name}")[:body]['cmd-delete'], 'Expected tube cmd-delete to be incremented')
    end

  end

end
