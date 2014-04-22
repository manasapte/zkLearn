module ZkRecipes 
  module Election
    class Candidate
      inclide MonitorMixin

      def initialize(zk, identity, data, leader_path, handler, notifier)
        @zk               = zk
        @handler          = handler
        @identity         = identity
        @data             = data
        @leader_path      = '/' + leader_path
        @candidate_prefix = '_candidate_'
        @notifier         = notifier
        mon_initialize
      end

      def log(msg)
        $stderr.puts("candidate[#{::Process.pid}] | #{msg}")
      end

      def current_candidates
        @zk.children("/", :watch => false).grep(/^#{@candidate_prefix}/).sort
      end

      def clear_parent_subscription
        if @parent_subscription
          @parent_subscription.unregister
        end
      end

      def set_parent_subscription(parent_path)
        clear_parent_subscription
        @parent_subscription = @zk.register(parent_path) do |event|
          handle_parent_event(event)
        end
      end

      def subscribe_to_leader
        @leader_subscription = @zk.register(@leader_path) do |event|
          handle_leader_event(event)
        end
        watch_leader
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

      def leader_ready 
        @zk.create(@leader_path, :data => @data, :mode => :ephemeral)
      end

      def handle_parent_event(event)
        if event.node_deleted?
          run_election
        end
      end

      def handle_leader_event(event)
        log("in handle leader event")
        if event.node_created?
          log("node created callback")
          data = @zk.get(event.path)[0]
          log("node created callback get done")
          handle_leader_ready(data) if !i_the_leader?
        end
      rescue ZK::Exceptions::NoNode => e
      ensure
        watch_leader
      end

      def watch_leader
        log("trying to watch leader")
        @zk.stat(@leader_path, :watch => true)
        log("done watching leader")
      end

      def attempt_watch_parent(parent_path)
        success = false
        set_parent_subscription(parent_path)
        @zk.exists?(parent_path, :watch => true)
      end

      def wait_for_next_round
        @candidates[0, my_index].reverse.each do |path|
          if attempt_watch_parent("/" + path)
            log("setting the parent watch on path: /#{path}")
            return
          end
        end
        clear_parent_subscription
        @all_parents_dead = true
        election_won
      end

      def handle_leader_ready(data)
        log("attempting to call leader ready")
        synchronize do
          log("calling leader ready HASDIUBHASDTYJVUYBEXRCTYVURFTYGU")
          @handler.leader_ready(data)
        end
        log("HANDLE LREADER READU IS FUCKING DONE")
      end

      def find_current_leader
        if @zk.exists?(@leader_path)
          data = @zk.get(@leader_path)
          handle_leader_ready(data[0])
        end
      rescue ZK::Exceptions::NoNode => e
      end

      def listen_on_connection
        @zk.on_state_change do |e|
          if e.connecting?
            log("zookeeper_client_connecting")
          elsif e.associating?
            log("zookeeper_client_associating")
          elsif e.connected?
            log("zookeeper_client_connected")
          elsif e.auth_failed?
            log("zookeeper_client_auth_failed")
          elsif e.expired_session?
            log("zookeeper_client_expired_session")
          else
            log("zookeeper_state_change")
          end
        end
      end

      def start
        log("candidate started with identity #{@identity}")
        listen_on_connection
        subscribe_to_leader
        become_a_candidate
        run_election
        unless i_the_leader? 
          find_current_leader
        end
        begin
          sleep
        rescue Interrupt
        end
      end

      def candidate_prefix_path
        "/" + @candidate_prefix
      end

      def become_a_candidate
        @path = @zk.create(candidate_prefix_path, :data => @data, :mode => :ephemeral_sequential)
      end

      def election_won
        log("election won trying for the mutex")
        synchronize do
          log("election won got the mutex")
          clear_leader_subscription
          log("calling election won now on the handler")
          @handler.election_won
          leader_ready
        end
      end

      def election_lost
        synchronize do
          @handler.election_lost
        end
      end

      def run_election 
        @candidates = current_candidates 
        if i_the_leader?
          log("I WON!")
          election_won
        else
          log("I LOST!")
          election_lost
          wait_for_next_round
        end
      end
    end
  end
end
