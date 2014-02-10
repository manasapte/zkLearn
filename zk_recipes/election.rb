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

      def clear_parent_watch
        if @parent_watch
          @parent_watch.unregister
        end
      end

      def i_the_leader?
        election_nodes[0] == @path
      end

      def get_parent_path!
        return unless @started
        nodes = election_nodes
        idx   = nodes.index[@path] 
        return if idx == 0
        @parent_path = nodes[idx - 1]
      end

      def watch_parent
        clear_parent_watch
        @zk.register(parent_path) do |event|
          if event.node_deleted?
            if i_the_leader?
              handler.election_won!
            else
              watch_parent

      def start
        @started = true
        @path = @zk.create(:mode => :ephemeral_sequential)
        if i_the_leader?
          handler.election_won!
        else
          p_path = parent_path
          @parent_watch = @zk.register do |event|
            if event.node_deleted?
              am_i_the_new_leader
            end
          end
          @heartbeat.start
        end
      end


