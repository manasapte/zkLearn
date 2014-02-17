module ZkRecipes
  module Election
    class ElectionCandidate
      def initialize(zk, app_name, data, election_ns, handler)
        @zk                 = zk
        @namespace          = app_name
        @election_ns        = election_ns
        @handler            = handler
        @data               = data
        @prefix             = get_absolute_path([@namespace, @election_ns])
        @started            = false
        @app_event_mutex    = Mutex.new 
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
        @zk.children(@prefix, :watch => false).grep(/^_candidate_/).sort
      end

      def clear_parent_subscription
        if @parent_subscription
          @parent_subscription.unregister
        end
      end

      def set_parent_subscription
        clear_parent_subscription
        @parent_subscription = @zk.register(parent_path) do |event|
          handle_parent_event(event)
        end
      end

      def set_leader_subscription
        clear_leader_subscription
        @leader_subscription = @zk.register(@leader_path) do |event|
          handle_leader_event(event)
          watch_leader
        end
      end

      def clear_leader_subscription
        if @leader_subscription
          @leader_subscription.unregister
        end
      end

      def my_index
        @candidates.index(File.basename(@path))
      end

      def i_the_leader?
        my_index == 0 || @all_parents_dead
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
          run_election
        end
      end

      def handle_leader_event(event)
        if event.node_created?
          data = @zk.get(event.path)[0]
          handle_leader_ready(data) if !i_the_leader?
          @zk.stat(@leader_path, :watch => true)
        end
      rescue ZK::Exceptions::NoNode => e
      end

      def watch_leader
        @zk.stat(@leader_path, :watch => true)
      end

      def attempt_watch_parent(parent_path)
        success = false
        set_parent_subscription
        if !@zk.exists?(parent_path, :watch => true)
          clear_parent_suscription
          false
        else
          p "watch succeeded for path: #{parent_path}"
          true
        end
      end

      def wait_for_next_round
        clear_parent_subscription
        @candidates[0, my_index].reverse.each do |path|
          p "ppath is: #{get_relative_path(@prefix, path)}"
          return if attempt_watch_parent(get_relative_path(@prefix, path))
        end
        @all_parents_dead = true
        election_won
      end

      def handle_leader_ready(data)
        @app_event_mutex.synchronize do
          @handler.leader_ready!(data)
        end
      end

      def find_current_leader
        if @zk.exists?(@leader_path)
          data = @zk.get(@leader_path)
          handle_leader_ready(data[0])
        end
      rescue ZK::Exceptions::NoNode => e
      end

      def start
        return if @started
        @started = true
        @waiting_for_next_round = false
        create_if_not_exists(get_absolute_path([@namespace]))
        create_if_not_exists(get_absolute_path([@namespace, @election_ns]))
        @leader_path = get_relative_path(@prefix, "current_leader")
        set_leader_subscription
        watch_leader
        become_a_candidate
        run_election
        unless i_the_leader? 
          find_current_leader
        end
      end

      def become_a_candidate
        @path = @zk.create(get_relative_path(@prefix, "_candidate_"), :data => @data, :mode => :ephemeral_sequential)
      end

      def stop
        return unless @started
      end

      def election_won
        @app_event_mutex.synchronize do
          clear_leader_subscription
          @handler.election_won!
          leader_ready
        end
      end

      def election_lost
        @app_event_mutex.synchronize do
          @handler.election_lost!
        end
      end

      def run_election 
        @candidates = current_candidates 
        if i_the_leader?
          election_won
        else
          election_lost
          wait_for_next_round
        end
      end
    end
  end
end
