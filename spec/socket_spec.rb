require 'eventmachine'
require 'json'
require 'multi_json'
require 'sensu/socket'

require 'helpers'

describe Sensu::Socket do
  include Helpers

  before(:each) do
    MultiJson.load_options = { :symbolize_keys => true }
  end

  subject { described_class.new(nil) }

  let(:logger) { double('Logger') }
  let(:transport) { double('Transport') }
  let(:check_report_data) do
    {
      :name => 'o-hai',
      :output => 'DEADBEEF' * 2,
      :status => 3,
    }
  end

  let(:settings) do
    {
      :client => {
        :name => 'example_client_name',
      },
    }
  end

  before(:each) do
    subject.logger = logger
    subject.settings = settings
    subject.transport = transport

    allow(Time).to receive_messages(:now => Time.at(1234))
  end

  describe '#receive_data' do
    #
    # Unit tests
    #
    it "responds 'invalid' there is a data error detected further in the processing chain" do
      expect(subject).to receive(:process_data).with('NONCE').and_raise(described_class::DataError, "OH NOES")
      expect(logger).to receive(:warn).with('OH NOES')
      expect(subject).to receive(:respond).with('invalid')

      subject.receive_data('NONCE')
    end

    #
    # Integration tests
    #
    it 'accepts data as part of an EM socket server' do
      async_wrapper do
        EventMachine::start_server('127.0.0.1', 303031, described_class) do |agent_socket|
          agent_socket.logger = logger
          agent_socket.settings = settings
          agent_socket.transport = transport

          expect(agent_socket).to receive(:respond).\
            with('ok') do
              after_watchdog_should_have_fired = 1.1 * described_class::WATCHDOG_DELAY
              timer(after_watchdog_should_have_fired) { async_done}
            end
        end

        expect(logger).not_to receive(:warn)

        payload = { :client => 'example_client_name', :check => check_report_data.merge(:issued => 1234) }

        expect(logger).to receive(:info).\
          with('publishing check result', { :payload => payload})

        expect(transport).to receive(:publish).\
          with(:direct, 'results', kind_of(String)) do |_, _, json_string|
            expect(MultiJson.load(json_string)).to eq payload
          end

        timer(0.1) do
          EventMachine.connect('127.0.0.1', 303031) do |socket|

            #
            # Send data one byte at a time.
            #

            pending = check_report_data.to_json.chars.to_a

            EventMachine.tick_loop do
              if pending.empty?
                :stop
              else
                expect(logger).to receive(:debug).with("socket received data", kind_of(Hash))
                socket.send_data(pending.shift)
              end
            end
          end
        end
      end
    end

    it 'will give up on receiving data from a client that has stopped sending for too long' do
      # If this test times out it is because the implementation is incorrect.
      async_wrapper do
        EventMachine::start_server('127.0.0.1', 303030, described_class) do |agent_socket|
          agent_socket.logger = logger
          agent_socket.settings = settings
          agent_socket.transport = transport

          expect(agent_socket).to receive(:respond).with('invalid') { async_done }
        end

        allow(logger).to receive(:debug)
        expect(logger).to receive(:warn).with(
          'giving up on data buffer from client',
          kind_of(Hash)
        )

        timer(0.1) do
          EventMachine.connect('127.0.0.1', 303030) do |socket|
            socket.send_data(%({"partial":))
          end # EventMachine.connect(...)
        end # timer
      end # async_wrapper
    end # it
  end # describe

  describe '#process_data' do
    it 'detects non-ASCII characters' do
      expect { subject.process_data("\x80\x88\x99\xAA\xBB") }.to\
        raise_error(described_class::DataError, 'socket received non-ascii characters')
    end

    it 'responds to a `ping`' do
      expect(logger).to receive_messages(:debug => 'socket received ping')
      expect(subject).to receive_messages(:respond => 'pong')

      subject.process_data('  ping  ')
    end

    it 'debug-logs data blobs passing through it' do
      expect(logger).to receive(:debug).with('socket received data', :data => 'a relentless stream of garbage' )
      subject.process_data('a relentless stream of garbage')
    end
  end

  describe '#process_json' do
    it 'must be valid json' do
      expect { subject.process_json('a relentless stream of garbage') }.to\
        raise_error(
          described_class::DataError,
          /check result is not valid json: error: \d+: unexpected token at 'a relentless stream of garbage'/
        )
    end

    it 'publishes valid check results' do
      expect(described_class).to receive(:validate_check_data).with(check_report_data)
      expect(subject).to receive(:publish_check_data).with(check_report_data)

      subject.process_json(check_report_data.to_json)
    end
  end

  describe '#publish_check_data' do
    it 'publishes check data' do
      payload = { :client => 'example_client_name', :check => { :o => :lol, :issued => 1234 } }

      expect(logger).to receive(:info).with('publishing check result', { :payload => payload })
      expect(transport).to receive(:publish).with(:direct, 'results', payload.to_json)

      subject.publish_check_data({:o => :lol})
    end
  end

  describe '.validate_check_data' do
    it 'must contain a non-empty check name' do
      check_report_data.merge!(:name => '')

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, "invalid check name: ''")
    end

    it 'must contain an acceptable check name' do
      check_report_data.merge!(:name => 'o hai')

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, "invalid check name: 'o hai'")
    end

    it 'must have check output that is a string' do
      check_report_data.merge!(:output => 1234)

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, 'check output must be a String, got Fixnum instead')
    end

    it 'must have an integer status' do
      check_report_data.merge!(:status => '1234')

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, 'check status must be an Integer, got String instead')
    end

    it 'must have a status code in the valid range' do
      check_report_data.merge!(:status => -2)

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, 'check status must be in {0, 1, 2, 3}, got -2 instead')

      check_report_data.merge!(:status => 4)

      expect { described_class.validate_check_data(check_report_data) }.to\
        raise_error(described_class::DataError, 'check status must be in {0, 1, 2, 3}, got 4 instead')
    end
  end
end
