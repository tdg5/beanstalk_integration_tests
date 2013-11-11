require 'test_helper'

class QuitTest < BeanstalkIntegrationTest

  context 'quit' do

    should 'close the connection' do
      other_client = build_client
      initial_current_connections = client.transmit('stats')[:body]['current-connections']
      # Send quit directly because transmit will die.
      other_client.connection.write("quit\r\n")
      # Make sure there are no further responses
      other_client.connection.write("stats\r\n")
      begin
        assert_nil other_client.connection.gets
      rescue Errno::ECONNRESET
        # Occasionally occurs after disconnect
      end
      assert_equal(initial_current_connections - 1, client.transmit('stats')[:body]['current-connections'], 'Expected current-connections to be decremented after quit')
    end

  end

end
