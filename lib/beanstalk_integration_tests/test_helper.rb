require 'test/unit'
require 'mocha/setup'
require 'minitest/autorun'
require 'minitest/should'

require 'securerandom'

require 'beaneater'


class BeanstalkIntegrationTest < MiniTest::Should::TestCase

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
      client.close unless client.connection.nil?
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
