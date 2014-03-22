module ServiceDiscovery
  module Collectors
    class ZookeeperCollector
      class Zookeeper
        def initialize(zk, notifier)
          @zk                = zk
          @notifier          = notifier
          @ready             = false
          manage_connection
        end

        def manage_connection
          @zk.on_state_change do |e|
            if e.connecting?
              @notifier.connecting
            elsif e.associating?
              @notifier.associating
            elsif e.connected?
              @notifier.connected
              @healing_handler.call(e, root) if @healing_handler
            elsif e.auth_failed?
              @notifier.auth_failed
            elsif e.expired_session?
              @notifier.expired_session
              @partition_handler.call(e, root) if @partition_handler
            else
              @notifier.state_change(e)
            end
          end
        end

        def on_network_partition(&block)
          @partition_handler = block
        end

        def on_network_healing(&block)
          @healing_handler = block
        end

        def root(&handler)
          @root ||= Znode.new(@zk, nil, nil, @notifier)
        end

        def get_or_create(parent, name, data, mode = nil)
          $stderr.puts("get or create for #{name}")
          parent.sync_children
          parent.children[name] || Znode.new(@zk, parent, name, @notifier).tap do |newnode|
            newnode.create(data, mode)
          end
        end
      end
    end
  end
end
