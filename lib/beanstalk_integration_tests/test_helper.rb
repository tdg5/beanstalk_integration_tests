require 'test/unit'
require 'mocha/setup'
require 'minitest/autorun'
require 'minitest/should'

require 'securerandom'
require 'timeout'

require 'beaneater'


# Open TCP connection with beanstalkd server prevents
# JRuby timeout from killing thread. Has some weird
# side effects, but allows Timeout#timeout to work
if RUBY_PLATFORM == 'java'
  module Timeout
    def timeout(sec, klass=nil)
      return yield(sec) if sec == nil or sec.zero?
      thread = Thread.new { yield(sec) }

      if thread.join(sec).nil?
        java_thread = JRuby.reference(thread)
        thread.kill
        java_thread.native_thread.interrupt
        thread.join(0.15)
        raise (klass || Error), 'execution expired'
      else
        thread.value
      end
    end
  end
end


class BeanstalkIntegrationTest < MiniTest::Should::TestCase
  include Timeout

  class << self

    def address(custom_address = '0.0.0.0')
      return @address ||= custom_address
    end

  end

  setup do
    puts "#{self.class}::#{self.__name__}" if ENV['VERBOSE']
  end

  teardown do
    cleanup_tubes
    @clients.each do |client|
      begin
        client.close unless client.connection.nil?
      rescue Errno::EBADF
        # Timeouts in JRuby trash the client connection
      end
    end
  end

  def address
    return self.class.address
  end

  def build_client
    new_client = Beaneater::Connection.new(address)
    @clients << new_client
    return new_client
  end

  def cleanup_tubes
    @pool = Beaneater::Pool.new([address])
    @tubes.each do |tube_name|
      @pool.tubes.find(tube_name).clear
    end
    @pool.close
    @tubes.clear
  end

  def client
    @client ||= build_client
  end

  def create_buried_jobs(buried_count = 5)
    job_ids = []
    buried_count.times do
      message = uuid
      client.transmit("put 0 0 120 #{message.bytesize}\r\n#{message}")
      timeout(1) do
        job_ids << client.transmit('reserve')[:id]
      end
      client.transmit("bury #{job_ids.last} 0")
    end
    return job_ids
  end

  def initialize(*)
    @tubes = []
    @clients = []
    super
  end

  def generate_tube_name
    tube = uuid
    @tubes << tube
    return tube
  end

  def tube_name
    return @tube_name ||= generate_tube_name
  end

  def uuid
    SecureRandom.uuid
  end

end
