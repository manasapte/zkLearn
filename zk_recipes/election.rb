module ZkRecipes
  module Election
    require 'heartbeat'
    HEARTBEAT_PERIOD = 2
    class ElectionCandidate
      def initialize(zk, app_name, election_ns, handler)
        @zk          = zk
        @namespace   = app_name
        @election_ns = election_ns
        @handler     = handler
        @prefix      = get_path([@namespace, @election_ns, "_vote_"]))
        @heartbeat   = ZkRecipes::Heartbeat.new(@zk, @namespace, HEARTBEAT_PERIOD)
        @started     = false
      end

      def get_path(crumbs)
        File.join([""] + crumbs)
      end

      def election_nodes
        @zk.children(get_path([@zk, @election_ns])).grep(/^_election_/).sort
      end

      def clear_parent_subscription
        if @parent_subscription
          @parent_subscription.unregister
        end
      end

      def i_the_leader?
        election_nodes[0] == @path
      end

      def parent_path
        nodes = election_nodes
        nodes.index[@path] == 0 ? nil : nodes[nodes.index(@path) - 1]
      end

      def handle_parent_event(event)
      end

      def watch_parent
        clear_parent_subscription
        loop do
          p_path = parent_path
          break if p_path.nil?
          @parent_subscription = @zk.register(p_path) do |event|
            handle_parent_event(event)
          end
          if !@zk.exists?(p_path, :watch => true) 
            clear_parent_suscription
          else
            break
          end
        end
      end

      def start
        @started = true
        @path = @zk.create(:mode => :ephemeral_sequential)
        loop do
          if i_the_leader?
            handler.election_won!
          else
            watch_parent
            @heartbeat.start
          end
        end
      end


