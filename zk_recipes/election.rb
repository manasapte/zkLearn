module ZkRecipes
  module Election
    require_relative 'heartbeat'
    HEARTBEAT_PERIOD = 2
    class ElectionCandidate
      def initialize(zk, app_name, data, election_ns, handler)
        @zk                 = zk
        @namespace          = app_name
        @election_ns        = election_ns
        @handler            = handler
        @data               = data
        @prefix             = get_absolute_path([@namespace, @election_ns])
        @heartbeat          = ZkRecipes::Heartbeat.new(@zk, HEARTBEAT_PERIOD)
        @started            = false
        @leader_ready_mutex = Mutex.new 
      end

      def get_absolute_path(crumbs)
        File.join([""] + crumbs)
      end

      def get_relative_path(prefix, suffix)
        File.join([prefix, suffix])
      end

      def create_if_not_exists(path)
        unless @zk.exists?(path)
          @zk.create(path)
        end
      end

      def current_candidates
        @zk.children(@prefix, :watch => false).grep(/^_vote_/).sort
      end

      def clear_parent_subscription
        if @parent_subscription
          @parent_subscription.unregister
        end
      end

      def clear_leader_subscription
        if @leader_subscription
          @leader_subscription.unregister
        end
      end

      def i_the_leader?
        @candidates[0] == File.basename(@path)
      end

      def parent_path
        idx = @candidates.index(File.basename(@path))
        idx == 0 ? nil : get_relative_path(@prefix, @candidates[idx - 1])
      end

      def leader_ready 
        @zk.create(@leader_path, :data => @data, :mode => :ephemeral)
      end

      def handle_parent_event(event)
        if event.node_deleted?
          check_results
        end
      end

      def handle_leader_event(event)
        if event.node_created?
          begin
            data = @zk.get(event.path)[0]
          rescue ZK::Exceptions::NoNode => e
          end
          call_leader_ready(data) if data
          @zk.stat(@leader_path, :watch => true)
        end
      end

      def watch_leader
        clear_leader_subscription
        @leader_subscription = @zk.register(@leader_path) do |event|
          handle_leader_event(event)
          watch_leader
        end
        @zk.stat(@leader_path, :watch => true)
      end

      def watch_parent(p_path)
        @parent_subscription = @zk.register(p_path) do |event|
          handle_parent_event(event)
        end
        if !@zk.stat(p_path, :watch => true).exists
          clear_parent_suscription
          false
        else
          true
        end
      end

      def wait_for_next_election 
        clear_parent_subscription
        loop do
          p_path = parent_path
          if p_path.nil?
            election_won
            break
          end
          break if watch_parent(p_path)
        end
      end

      def call_leader_ready(data)
        @leader_ready_mutex.synchronize do
          @handler.leader_ready!(data)
        end
      end

      def find_current_leader
        if @zk.stat(@leader_path).exists
          begin
            data = @zk.get(@leader_path)
            call_leader_ready(data[0])
          rescue ZK::Exceptions::NoNode => e
          end
        end
      end

      def start
        return if @started
        @started = true
        @heartbeat.start
        create_if_not_exists(get_absolute_path([@namespace]))
        create_if_not_exists(get_absolute_path([@namespace, @election_ns]))
        @leader_path = get_relative_path(@prefix, "current_leader")
        watch_leader
        vote!
        unless check_results
          find_current_leader
        end
        @heartbeat.join
      end

      def vote!
        @path = @zk.create(get_relative_path(@prefix, "_vote_"), :data => @data, :mode => :ephemeral_sequential)
      end

      def stop
        return unless @started
        @heartbeat.stop
      end

      def election_won
        clear_leader_subscription
        @handler.election_won!
        leader_ready
      end

      def election_lost
        @handler.election_lost!
        wait_for_next_election 
      end

      def check_results
        @candidates = current_candidates 
        if i_the_leader?
          election_won
          true
        else
          election_lost
          false
        end
      end
    end
  end
end
