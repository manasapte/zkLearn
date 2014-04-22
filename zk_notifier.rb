module ServiceDiscovery
  module Collectors
    class ZookeeperCollector
      class ZkNotifier 
        def initialize(io)
          @io = io
        end

        def connecting(&block)
          @io << "zookeeper client connecting"
        end

        def associating(&block)
          @io << "zookeeper client associating"
        end

        def connected(&block)
          @io << "zookeeper client connected"
        end

        def auth_failed(&block)
          @io << "zookeeper client auth failed"
        end

        def expired_session(&block)
          @io << "zookeeper client expired session"
        end

        def state_change(&block)
          @io << "zookeeper client state change"
        end

        def not_connected(&block)
          @io << "zookeeper client state change"
          @io << " : #{yield if block_given?}"
        end

        def failed(e)
          @io << "exception #{e.message} and backtrace: #{e.backtrace.join("\n")}"
        end
      end
    end
  end
end
